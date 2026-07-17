package com.sanjay.ftgo.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FtgoConsumerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoConsumerServiceApplication.class, args);
    }
}
