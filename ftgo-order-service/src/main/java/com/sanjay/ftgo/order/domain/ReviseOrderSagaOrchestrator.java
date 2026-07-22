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

// Deliberately stateless, like CancelOrderSagaOrchestrator: Revise Order is a strict linear
// pipeline (kitchen re-check -> accounting re-authorize -> confirm/reject), and both the pending
// revised quantity and the original quantity are recomputed from Order's own line items rather
// than threaded through the reply chain, so no persisted saga instance table is needed either.
@Service
public class ReviseOrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(ReviseOrderSagaOrchestrator.class);

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final OrderDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public ReviseOrderSagaOrchestrator(OrderRepository orderRepository,
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
        int newTotalQuantity = totalQuantity(order.getPendingRevisedLineItems());
        String eventId = UUID.randomUUID().toString();
        publishKitchenCommand(eventId, "ReviseTicket", order.getId(), newTotalQuantity);
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
        switch (eventType) {
            case "TicketRevisionRejected", "TicketRevisionUndone" -> rejectRevision(orderId);
            case "TicketQuantityRevised" -> tryAuthorize(orderId);
            default -> { }
        }
    }

    private void tryAuthorize(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        int newTotalQuantity = totalQuantity(order.getPendingRevisedLineItems());
        String eventId = UUID.randomUUID().toString();
        publishAccountingCommand(eventId, "ReviseAuthorization", orderId, newTotalQuantity);
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        switch (eventType) {
            case "AuthorizationRevised" -> confirmRevision(orderId);
            case "AuthorizationRevisionRejected" -> sendUndoReviseTicket(orderId);
            default -> { }
        }
    }

    private void sendUndoReviseTicket(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        int originalTotalQuantity = totalQuantity(order.getLineItems());
        String eventId = UUID.randomUUID().toString();
        publishKitchenCommand(eventId, "UndoReviseTicket", orderId, originalTotalQuantity);
    }

    private void confirmRevision(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.confirmRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision confirmation for order {}: {}", orderId, e.getMessage());
        }
    }

    private void rejectRevision(Long orderId) {
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return;
        }
        try {
            List<OrderDomainEvent> events = order.rejectRevision();
            orderRepository.save(order);
            domainEventPublisher.publish(events);
        } catch (UnsupportedStateTransitionException e) {
            log.debug("Ignoring revision rejection for order {}: {}", orderId, e.getMessage());
        }
    }

    private int totalQuantity(List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }

    private void publishKitchenCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        outboxEventRepository.save(new OutboxEvent(eventId, commandType, orderId, "kitchen.commands",
                toJson(new KitchenCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"))));
    }

    private void publishAccountingCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        outboxEventRepository.save(new OutboxEvent(eventId, commandType, orderId, "accounting.commands",
                toJson(new AccountingCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"))));
    }

    private String toJson(Object command) {
        try {
            return objectMapper.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga command", e);
        }
    }
}
