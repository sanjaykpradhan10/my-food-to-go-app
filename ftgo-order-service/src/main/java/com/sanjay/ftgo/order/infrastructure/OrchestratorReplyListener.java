package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.CancelOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.SagaReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Single consumer group on saga.replies shared by both orchestrators: Kafka replies carry
// a sagaType field (Task 1) so this listener can route to the right orchestrator before
// either one's handleReply is invoked, keeping CreateOrderSagaOrchestrator untouched.
@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestratorReplyListener {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorReplyListener.class);

    private final CreateOrderSagaOrchestrator createOrderSagaOrchestrator;
    private final CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator;
    private final ObjectMapper objectMapper;

    public OrchestratorReplyListener(CreateOrderSagaOrchestrator createOrderSagaOrchestrator,
                                      CancelOrderSagaOrchestrator cancelOrderSagaOrchestrator,
                                      ObjectMapper objectMapper) {
        this.createOrderSagaOrchestrator = createOrderSagaOrchestrator;
        this.cancelOrderSagaOrchestrator = cancelOrderSagaOrchestrator;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "saga.replies", groupId = "order-service")
    public void onMessage(String payload) {
        SagaReply reply;
        try {
            reply = objectMapper.readValue(payload, SagaReply.class);
        } catch (Exception e) {
            log.warn("Skipping malformed saga reply: {}", payload, e);
            return;
        }
        switch (reply.sagaType()) {
            case "CreateOrder" -> createOrderSagaOrchestrator.handleReply(
                    reply.eventId(), reply.participant(), reply.eventType(), reply.orderId(), reply.reason());
            case "CancelOrder" -> cancelOrderSagaOrchestrator.handleReply(
                    reply.eventId(), reply.participant(), reply.eventType(), reply.orderId(), reply.reason());
            default -> log.warn("Unknown saga type on reply: {}", reply.sagaType());
        }
    }
}
