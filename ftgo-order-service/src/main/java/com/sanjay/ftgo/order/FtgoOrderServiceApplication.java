package com.sanjay.ftgo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FtgoOrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoOrderServiceApplication.class, args);
    }
}
