package com.sanjay.ftgo.order.eventsourcing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.order.domain.SagaCommandPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedSagaCommandPublisher implements SagaCommandPublisher {

    private final OrderSagaCommandRequestRepository repository;
    private final ObjectMapper objectMapper;

    public EventSourcedSagaCommandPublisher(OrderSagaCommandRequestRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(String topic, String eventId, String eventType, Long orderId, Object command) {
        repository.save(new OrderSagaCommandRequest(eventId, eventType, orderId, topic, toJson(command)));
    }

    private String toJson(Object command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga command", e);
        }
    }
}
