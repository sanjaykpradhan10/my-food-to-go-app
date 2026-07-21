package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

// Deliberately stateless, unlike CreateOrderSagaOrchestrator: Cancel Order is a strict
// linear pipeline (kitchen cancel -> accounting reversal -> order cancelled) with no
// parallel replies to join, so there's no need for a persisted saga instance table.
@Service
public class CancelOrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(CancelOrderSagaOrchestrator.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public CancelOrderSagaOrchestrator(OrderRepository orderRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        OutboxEventRepository outboxEventRepository,
                                        OrderDomainEventPublisher domainEventPublisher,
                                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void start(Order order) {
        String eventId = UUID.randomUUID().toString();
        publishCommand("kitchen.commands", eventId, "CancelTicket", order.getId(),
                new KitchenCommand(eventId, "CancelTicket", order.getId(), null, "CancelOrder"));
    }

    @Transactional
    public void handleReply(String eventId, String participant, String eventType, Long orderId, String reason) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        switch (participant) {
            case "kitchen" -> handleKitchenReply(eventType, orderId);
            case "accounting" -> handleAccountingReply(eventType, orderId);
            default -> { }
        }
    }

    private void handleKitchenReply(String eventType, Long orderId) {
        if ("TicketCancellationRejected".equals(eventType)) {
            undoCancel(orderId);
            return;
        }
        String eventId = UUID.randomUUID().toString();
        publishCommand("accounting.commands", eventId, "ReverseAuthorization", orderId,
                new AccountingCommand(eventId, "ReverseAuthorization", orderId, null, "CancelOrder"));
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        if ("AuthorizationReversed".equals(eventType)) {
            confirmCancel(orderId);
        }
    }

    private void confirmCancel(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.noteCancelled();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    private void undoCancel(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.undoCancel();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring cancel rejection for order {}: {}", orderId, e.getMessage());
        }
    }

    private void publishCommand(String topic, String eventId, String eventType, Long orderId, Object command) {
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, topic, toJson(command)));
    }

    private String toJson(Object command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga command", e);
        }
    }
}
