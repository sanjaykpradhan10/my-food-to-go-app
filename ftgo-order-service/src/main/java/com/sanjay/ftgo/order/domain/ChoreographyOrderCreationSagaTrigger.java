package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "saga.mode", havingValue = "choreography", matchIfMissing = true)
public class ChoreographyOrderCreationSagaTrigger implements OrderCreationSagaTrigger {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public ChoreographyOrderCreationSagaTrigger(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onOrderCreated(Order order, String eventId) {
        OrderCreatedEvent event = OrderCreatedEvent.from(order, eventId);
        outboxEventRepository.save(new OutboxEvent(eventId, "OrderCreated", order.getId(), "order.events", toJson(event)));
    }

    private String toJson(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderCreatedEvent for order " + event.orderId(), e);
        }
    }
}
