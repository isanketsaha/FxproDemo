package com.finance.ohlc.service;


import com.finance.ohlc.config.HourSizeConfig;
import com.finance.ohlc.domain.OhlcStage;
import com.finance.ohlc.service.interfaces.OhlcService;
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

    private CalculationService calculationService;

    private AsyncItemWriter<OhlcStage> asyncItemWriter;

    @Bean
    public Job jobPerHour() {
        return this.jobBuilderFactory.get("jobPerHour")
                .start(jobPerHourSteps()).incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step jobPerHourSteps() {
        return this.stepBuilderFactory.get("jobPerHourSteps")
                .<OhlcStage, Future<OhlcStage>>chunk(hourSizeConfig())
                .reader(itemHourReader())
                .processor(asyncHourItemProcessor())
                .writer(asyncItemWriter).taskExecutor(new SimpleAsyncTaskExecutor())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public ItemReaderAdapter<OhlcStage> hourReader() {
        ItemReaderAdapter<OhlcStage> adapter = new ItemReaderAdapter<>();
        adapter.setTargetObject(calculationService);
        adapter.setTargetMethod("fetchIncomingQuotesForHour");
        return adapter;
    }

    @Bean
    public SingleItemPeekableItemReader<OhlcStage> itemHourReader() {
        SingleItemPeekableItemReader<OhlcStage> reader = new SingleItemPeekableItemReader<>();
        reader.setDelegate(hourReader());
        return reader;
    }

    @Bean
    public ItemProcessor<OhlcStage, OhlcStage> itemHourProcessor() {
        ItemProcessorAdapter<OhlcStage, OhlcStage> adapter = new ItemProcessorAdapter<>();
        adapter.setTargetObject(calculationService);
        adapter.setTargetMethod("processHour");
        return adapter;
    }

    @Bean
    public AsyncItemProcessor<OhlcStage, OhlcStage> asyncHourItemProcessor() {
        AsyncItemProcessor<OhlcStage, OhlcStage> processor = new AsyncItemProcessor<>();
        processor.setDelegate(itemHourProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return processor;
    }

    @Bean
    public CompletionPolicy hourSizeConfig() {
        return new HourSizeConfig();
    }

}
