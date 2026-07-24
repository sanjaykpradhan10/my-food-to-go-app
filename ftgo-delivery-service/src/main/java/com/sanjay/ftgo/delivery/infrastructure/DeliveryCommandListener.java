package com.sanjay.ftgo.delivery.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.delivery.domain.DeliveryCommand;
import com.sanjay.ftgo.delivery.domain.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class DeliveryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(DeliveryCommandListener.class);

    private final DeliveryService deliveryService;
    private final ObjectMapper objectMapper;

    public DeliveryCommandListener(DeliveryService deliveryService, ObjectMapper objectMapper) {
        this.deliveryService = deliveryService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "delivery.commands", groupId = "delivery-service")
    public void onMessage(String payload) {
        DeliveryCommand command;
        try {
            command = objectMapper.readValue(payload, DeliveryCommand.class);
        } catch (Exception e) {
            log.warn("Skipping malformed delivery command: {}", payload, e);
            return;
        }
        switch (command.commandType()) {
            case "ScheduleDelivery" ->
                    deliveryService.handleScheduleDeliveryCommand(command.eventId(), command.orderId(), command.restaurantId());
            case "ReleaseDelivery" ->
                    deliveryService.handleReleaseDeliveryCommand(command.eventId(), command.orderId(), command.sagaType());
            default -> log.warn("Unknown delivery command type: {}", command.commandType());
        }
    }
}
