package com.sanjay.ftgo.kitchen;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FtgoKitchenServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtgoKitchenServiceApplication.class, args);
    }
}
