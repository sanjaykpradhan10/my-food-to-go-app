package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.KitchenEvent;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class KitchenEventListener {

    private static final Logger log = LoggerFactory.getLogger(KitchenEventListener.class);

    private final OrderSagaService orderSagaService;
    private final OrderCancelSagaService orderCancelSagaService;
    private final ObjectMapper objectMapper;

    public KitchenEventListener(OrderSagaService orderSagaService,
                                 OrderCancelSagaService orderCancelSagaService,
                                 ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.orderCancelSagaService = orderCancelSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "kitchen.events", groupId = "order-service")
    public void onMessage(String payload) {
        KitchenEvent event;
        try {
            event = objectMapper.readValue(payload, KitchenEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed kitchen event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "TicketConfirmed" -> orderSagaService.approve(event.orderId(), event.eventId());
            case "TicketCreationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "TicketCancellationRejected" -> orderCancelSagaService.rejectCancel(event.orderId(), event.eventId());
            default -> { }
        }
    }
}
