package com.finance.ohlc.service;


import com.finance.ohlc.config.HourSizeConfig;
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
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemProcessorAdapter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.concurrent.Future;

@Configuration
@AllArgsConstructor
public class HourBatchService {


    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private OhlcService ohlcService;

    private ItemWriter<Ohlc> itemWriter;

    private CalculationService calculationService;

    private AsyncItemWriter<Ohlc> asyncItemWriter;

    @Bean
    public Job jobPerHour() {
        return this.jobBuilderFactory.get("jobPerHour")
                .start(jobPerHourSteps()).incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step jobPerHourSteps() {
        return this.stepBuilderFactory.get("jobPerHourSteps")
                .<Quote, Future<Ohlc>>chunk(hourSizeConfig())
                .reader(itemHourReader())
                .processor(asyncHourItemProcessor())
                .writer(asyncItemWriter).taskExecutor(new SimpleAsyncTaskExecutor())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReaderAdapter<Quote> hourReader() {
        ItemReaderAdapter<Quote> adapter = new ItemReaderAdapter<>();
        adapter.setTargetObject(ohlcService);
        adapter.setTargetMethod("fetchIncomingQuotesPerHour");
        return adapter;
    }

    @Bean
    public SingleItemPeekableItemReader<Quote> itemHourReader() {
        SingleItemPeekableItemReader<Quote> reader = new SingleItemPeekableItemReader<>();
        reader.setDelegate(hourReader());
        return reader;
    }

    @Bean
    public ItemProcessor<Quote, Ohlc> itemHourProcessor() {
        ItemProcessorAdapter<Quote, Ohlc> adapter = new ItemProcessorAdapter<>();
        adapter.setTargetObject(calculationService);
        adapter.setTargetMethod("processHour");
        return adapter;
    }

    @Bean
    public AsyncItemProcessor<Quote, Ohlc> asyncHourItemProcessor() {
        AsyncItemProcessor<Quote, Ohlc> processor = new AsyncItemProcessor<>();
        processor.setDelegate(itemHourProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }

    @Bean
    public CompletionPolicy hourSizeConfig() {
        return new HourSizeConfig();
    }

}
