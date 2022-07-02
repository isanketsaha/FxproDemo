package com.finance.ohlc.service;

import com.finance.ohlc.config.MinuteSizeConfig;
import com.finance.ohlc.domain.Ohlc;
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
                .<Quote, Future<Ohlc>>chunk(minuteSizeConfig())
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
        adapter.setTargetMethod("fetchIncomingQuotesPerMinute");
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
    public ItemProcessor<Quote, Ohlc> itemProcessor() {
        ItemProcessorAdapter<Quote, Ohlc> adapter = new ItemProcessorAdapter<>();
        adapter.setTargetObject(calculationService);
        adapter.setTargetMethod("processDay");
        return adapter;
    }

    @Bean
    public CompletionPolicy minuteSizeConfig() {
        return new MinuteSizeConfig();
    }

    @Bean
    public ItemWriterAdapter<Ohlc> itemWriter() {
        ItemWriterAdapter<Ohlc> itemWriterAdapter = new ItemWriterAdapter<>();
        itemWriterAdapter.setTargetObject(calculationService);
        itemWriterAdapter.setTargetMethod("logOhlc");
        return itemWriterAdapter;
    }

    @Bean
    public AsyncItemWriter<Ohlc> asyncItemWriter() {
        AsyncItemWriter<Ohlc> writer = new AsyncItemWriter<>();
        writer.setDelegate(itemWriter());
        return writer;
    }

    @Bean
    public AsyncItemProcessor<Quote, Ohlc> asyncItemProcessor() {
        AsyncItemProcessor<Quote, Ohlc> processor = new AsyncItemProcessor<>();
        processor.setDelegate(itemProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }
}
