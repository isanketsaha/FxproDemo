package com.finance.ohlc.service;

import com.finance.ohlc.config.MinuteSizeConfig;
import com.finance.ohlc.domain.Ohlc;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.service.interfaces.OhlcService;
import com.finance.ohlc.vm.Quote;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.adapter.ItemProcessorAdapter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.concurrent.Future;


/**
 * This Job will create OHLC data for each Minute period. The data will be injected from incoming quotes Queue.
 * Expectation is we will get high throughput, create chunk for each minute and process accordingly.
 * We are creating the reader , processor and writer as async for scaling our job.
 */
@Configuration
@AllArgsConstructor
public class MinuteBatchService {

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private OhlcService ohlcService;

    private CalculationService calculationService;


    @Bean
    public Job jobPerMinute() {
        return this.jobBuilderFactory.get("jobPerMinute")
                .start(jobPerMinuteSteps()).incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step jobPerMinuteSteps() {
        return this.stepBuilderFactory.get("jobPerMinuteSteps")
                .<Quote, Future<OhlcStage>>chunk(minuteSizeConfig())
                .reader(itemMinuteReader())
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter()).taskExecutor(new SimpleAsyncTaskExecutor())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReaderAdapter<Quote> minuteReader() {
        ItemReaderAdapter<Quote> adapter = new ItemReaderAdapter<>();
        adapter.setTargetObject(ohlcService);
        adapter.setTargetMethod("fetchIncomingQuotesForMinute");
        return adapter;
    }

    @Bean
    public SingleItemPeekableItemReader<Quote> itemMinuteReader() {
        SingleItemPeekableItemReader<Quote> reader = new SingleItemPeekableItemReader<>();
        reader.setDelegate(minuteReader());
        return reader;
    }

    @Bean
    public ItemProcessor<Quote, Ohlc> itemMinuteProcessor() {
        ItemProcessorAdapter<Quote, Ohlc> adapter = new ItemProcessorAdapter<>();
        adapter.setTargetObject(calculationService);
        adapter.setTargetMethod("processMinute");
        return adapter;
    }

    @Bean
    public ItemProcessor<Quote, OhlcStage> itemProcessor() {
        ItemProcessorAdapter<Quote, OhlcStage> adapter = new ItemProcessorAdapter<>();
        adapter.setTargetObject(calculationService);
        adapter.setTargetMethod("processDay");
        return adapter;
    }

    @Bean
    public CompletionPolicy minuteSizeConfig() {
        return new MinuteSizeConfig();
    }

    @Bean
    public ItemWriterAdapter<OhlcStage> itemWriter() {
        ItemWriterAdapter<OhlcStage> itemWriterAdapter = new ItemWriterAdapter<>();
        itemWriterAdapter.setTargetObject(calculationService);
        itemWriterAdapter.setTargetMethod("logOhlc");
        return itemWriterAdapter;
    }

    @Bean
    public AsyncItemWriter<OhlcStage> asyncItemWriter() {
        AsyncItemWriter<OhlcStage> writer = new AsyncItemWriter<>();
        writer.setDelegate(itemWriter());
        return writer;
    }

    @Bean
    public AsyncItemProcessor<Quote, OhlcStage> asyncItemProcessor() {
        AsyncItemProcessor<Quote, OhlcStage> processor = new AsyncItemProcessor<>();
        processor.setDelegate(itemProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }
}
