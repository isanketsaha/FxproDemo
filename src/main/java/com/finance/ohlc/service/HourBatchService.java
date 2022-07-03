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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.adapter.ItemProcessorAdapter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;


/**
 * This Job will create OHLC data for each Hour period. The data will be injected from Minute Job output Queue.
 * Expectation is we will get input each minute, create chunk for each hour and process accordingly.
 */
@Configuration
@AllArgsConstructor
public class HourBatchService {


    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private OhlcService ohlcService;

    private CalculationService calculationService;

    private ItemWriterAdapter<OhlcStage> itemWriter;

    @Bean
    public Job jobPerHour() {
        return this.jobBuilderFactory.get("jobPerHour")
                .start(jobPerHourSteps()).incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step jobPerHourSteps() {
        return this.stepBuilderFactory.get("jobPerHourSteps")
                .<OhlcStage, OhlcStage>chunk(hourSizeConfig())
                .reader(itemHourReader())
                .processor(itemHourProcessor())
                .writer(itemWriter).taskExecutor(new SimpleAsyncTaskExecutor())
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
    public CompletionPolicy hourSizeConfig() {
        return new HourSizeConfig();
    }

}
