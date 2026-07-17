package com.sanjay.ftgo.consumer.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.consumer.domain.ConsumerVerificationService;
import com.sanjay.ftgo.consumer.domain.VerifyConsumerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class VerifyConsumerCommandListener {

    private static final Logger log = LoggerFactory.getLogger(VerifyConsumerCommandListener.class);

    private final ConsumerVerificationService consumerVerificationService;
    private final ObjectMapper objectMapper;

    public VerifyConsumerCommandListener(ConsumerVerificationService consumerVerificationService, ObjectMapper objectMapper) {
        this.consumerVerificationService = consumerVerificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "consumer.commands", groupId = "consumer-service")
    public void onMessage(String payload) {
        VerifyConsumerCommand command;
        try {
            command = objectMapper.readValue(payload, VerifyConsumerCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed verify-consumer command: {}", payload, e);
            return;
        }
        consumerVerificationService.handleVerifyConsumerCommand(command.eventId(), command.orderId(), command.consumerId());
    }
}
