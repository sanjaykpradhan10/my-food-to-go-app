package com.sanjay.ftgo.consumer.api;

import com.sanjay.ftgo.consumer.domain.Consumer;
import com.sanjay.ftgo.consumer.domain.ConsumerRepository;
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

    public ConsumerController(ConsumerRepository consumerRepository) {
        this.consumerRepository = consumerRepository;
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ConsumerResponse createConsumer(@RequestBody CreateConsumerRequest request) {
        Consumer consumer = consumerRepository.save(new Consumer(request.name(), request.active()));
        return ConsumerResponse.from(consumer);
    }
}
