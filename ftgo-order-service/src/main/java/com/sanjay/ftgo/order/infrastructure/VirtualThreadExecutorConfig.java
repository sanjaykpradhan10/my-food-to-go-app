package com.sanjay.ftgo.order.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Virtual threads are a natural fit for the composite order-view query: 4 short-lived,
// blocking, I/O-bound downstream calls fired concurrently, with no pool-size tuning decision
// to make or justify (unlike a fixed-size ExecutorService).
@Configuration
public class VirtualThreadExecutorConfig {

    @Bean
    public ExecutorService orderViewExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
