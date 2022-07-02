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
public class DayBatchService {


    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private OhlcService ohlcService;

    private ItemWriter<Ohlc> itemWriter;

    private AsyncItemWriter<Ohlc> asyncItemWriter;

    private CalculationService calculationService;

    @Bean
    public Job jobPerDay() {
        return this.jobBuilderFactory.get("jobPerDay")
                .start(jobPerDaySteps()).incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step jobPerDaySteps() {
        return this.stepBuilderFactory.get("jobPerDaySteps")
                .<Quote, Future<Ohlc>>chunk(daySizeConfig())
                .reader(itemDayReader())
                .processor(asyncDayItemProcessor())
                .writer(asyncItemWriter).taskExecutor(new SimpleAsyncTaskExecutor())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReaderAdapter<Quote> dayReader() {
        ItemReaderAdapter<Quote> adapter = new ItemReaderAdapter<>();
        adapter.setTargetObject(ohlcService);
        adapter.setTargetMethod("fetchIncomingQuotesPerDay");
        return adapter;
    }

    @Bean
    public SingleItemPeekableItemReader<Quote> itemDayReader() {
        SingleItemPeekableItemReader<Quote> reader = new SingleItemPeekableItemReader<>();
        reader.setDelegate(dayReader());
        return reader;
    }

    @Bean
    public ItemProcessor<Quote, Ohlc> itemDayProcessor() {
        ItemProcessorAdapter<Quote, Ohlc> adapter = new ItemProcessorAdapter<>();
        adapter.setTargetObject(calculationService);
        adapter.setTargetMethod("processDay");
        return adapter;
    }

    @Bean
    public AsyncItemProcessor<Quote, Ohlc> asyncDayItemProcessor() {
        AsyncItemProcessor<Quote, Ohlc> processor = new AsyncItemProcessor<>();
        processor.setDelegate(itemDayProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }

    @Bean
    public CompletionPolicy daySizeConfig() {
        return new HourSizeConfig();
    }

}
