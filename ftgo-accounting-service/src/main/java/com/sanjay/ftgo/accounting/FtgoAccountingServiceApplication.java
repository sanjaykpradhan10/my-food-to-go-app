package com.sanjay.ftgo.accounting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FtgoAccountingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoAccountingServiceApplication.class, args);
    }
}
