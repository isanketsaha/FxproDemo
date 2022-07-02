package com.finance.ohlc;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class OhlcServiceApplication {


    public static void main(String[] args) {
        SpringApplication.run(OhlcServiceApplication.class, args);

    }


}
