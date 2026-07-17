package com.sanjay.ftgo.order.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.CreateOrderSagaOrchestrator;
import com.sanjay.ftgo.order.domain.SagaReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "saga.mode", havingValue = "orchestration")
public class OrchestratorReplyListener {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorReplyListener.class);

    private final CreateOrderSagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper;

    public OrchestratorReplyListener(CreateOrderSagaOrchestrator orchestrator, ObjectMapper objectMapper) {
        this.orchestrator = orchestrator;
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
        orchestrator.handleReply(reply.eventId(), reply.participant(), reply.eventType(), reply.orderId(), reply.reason());
    }
}
