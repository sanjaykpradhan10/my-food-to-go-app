package com.sanjay.ftgo.configserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

@SpringBootApplication
@EnableConfigServer
public class FtgoConfigServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(FtgoConfigServerApplication.class, args);
    }
}
