package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.AccountingEvent;
import com.sanjay.ftgo.order.domain.OrderCancelSagaService;
import com.sanjay.ftgo.order.domain.OrderReviseSagaService;
import com.sanjay.ftgo.order.domain.OrderSagaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class AccountingEventListener {

    private static final Logger log = LoggerFactory.getLogger(AccountingEventListener.class);

    private final OrderSagaService orderSagaService;
    private final OrderCancelSagaService orderCancelSagaService;
    private final OrderReviseSagaService orderReviseSagaService;
    private final ObjectMapper objectMapper;

    public AccountingEventListener(OrderSagaService orderSagaService,
                                    OrderCancelSagaService orderCancelSagaService,
                                    OrderReviseSagaService orderReviseSagaService,
                                    ObjectMapper objectMapper) {
        this.orderSagaService = orderSagaService;
        this.orderCancelSagaService = orderCancelSagaService;
        this.orderReviseSagaService = orderReviseSagaService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "accounting.events", groupId = "order-service")
    public void onMessage(String payload) {
        AccountingEvent event;
        try {
            event = objectMapper.readValue(payload, AccountingEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed accounting event: {}", payload, e);
            return;
        }
        switch (event.eventType()) {
            case "CardAuthorizationFailed" -> orderSagaService.reject(event.orderId(), event.eventId());
            case "AuthorizationReversed" -> orderCancelSagaService.confirmCancel(event.orderId(), event.eventId());
            case "AuthorizationRevised" -> orderReviseSagaService.confirmRevision(event.orderId(), event.eventId());
            case "AuthorizationRevisionRejected" -> orderReviseSagaService.compensateRevision(event.orderId(), event.eventId());
            default -> { }
        }
    }
}
