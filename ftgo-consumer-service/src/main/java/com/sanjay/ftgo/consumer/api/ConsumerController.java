package com.sanjay.ftgo.consumer.api;

import com.sanjay.ftgo.consumer.domain.Consumer;
import com.sanjay.ftgo.consumer.domain.ConsumerRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/consumers")
public class ConsumerController {

    private final ConsumerRepository consumerRepository;
    private final MeterRegistry meterRegistry;

    public ConsumerController(ConsumerRepository consumerRepository, MeterRegistry meterRegistry) {
        this.consumerRepository = consumerRepository;
        this.meterRegistry = meterRegistry;
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ConsumerResponse createConsumer(@RequestBody CreateConsumerRequest request) {
        Consumer consumer = consumerRepository.save(new Consumer(request.name(), request.active()));
        meterRegistry.counter("consumers_created").increment();
        return ConsumerResponse.from(consumer);
    }
}
