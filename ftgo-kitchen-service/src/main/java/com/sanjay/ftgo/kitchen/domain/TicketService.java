package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class TicketService {

    private static final int KITCHEN_CAPACITY_LIMIT = 20;

    private final TicketRepository ticketRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final FailedOrderRepository failedOrderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final TicketDomainEventPublisher domainEventPublisher;
    private final ObjectMapper objectMapper;

    public TicketService(TicketRepository ticketRepository,
                          ProcessedEventRepository processedEventRepository,
                          FailedOrderRepository failedOrderRepository,
                          OutboxEventRepository outboxEventRepository,
                          TicketDomainEventPublisher domainEventPublisher,
                          ObjectMapper objectMapper) {
        this.ticketRepository = ticketRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedOrderRepository = failedOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.domainEventPublisher = domainEventPublisher;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        int totalQuantity = totalQuantity(event.lineItems());

        if (failedOrderRepository.existsById(event.orderId())) {
            ticketRepository.save(Ticket.createCancelled(event.orderId()));
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            domainEventPublisher.publishCreationFailed(
                    new TicketCreationFailedEvent(event.orderId(), "order exceeds kitchen capacity"));
            return;
        }

        TicketCreationResult result = Ticket.createTicket(event.orderId(), totalQuantity);
        Ticket ticket = ticketRepository.save(result.ticket());
        domainEventPublisher.publish(ticket, result.events());
    }

    @Transactional
    public void handleAccountingEvent(String eventId, Long orderId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }

        List<TicketDomainEvent> events = "CardAuthorized".equals(eventType) ? ticket.confirm() : ticket.cancel();
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @Transactional
    public void handleConsumerVerificationFailed(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            List<TicketDomainEvent> events = ticket.cancel();
            ticketRepository.save(ticket);
            domainEventPublisher.publish(ticket, events);
        } else {
            failedOrderRepository.save(new FailedOrder(orderId));
        }
    }

    @Transactional
    public void handleCreateTicketCommand(String eventId, Long orderId, Integer totalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (totalQuantity == null) {
            publishReply("TicketCreationFailed", orderId, "totalQuantity is required", "CreateOrder");
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            publishReply("TicketCreationFailed", orderId, "order exceeds kitchen capacity", "CreateOrder");
            return;
        }

        TicketCreationResult result = Ticket.createTicket(orderId, totalQuantity);
        ticketRepository.save(result.ticket());
        publishReply("TicketCreated", orderId, null, "CreateOrder");
    }

    @Transactional
    public void handleConfirmTicketCommand(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            ticket.confirm();
            ticketRepository.save(ticket);
        }
    }

    @Transactional
    public void handleCancelTicketCommand(String eventId, Long orderId, String sagaType) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }
        try {
            ticket.cancel();
            ticketRepository.save(ticket);
            publishReply("TicketCancelled", orderId, null, sagaType);
        } catch (TicketCannotBeCancelledException | UnsupportedStateTransitionException e) {
            publishReply("TicketCancellationRejected", orderId, e.getMessage(), sagaType);
        }
    }

    @Transactional
    public void handleOrderCancelled(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }
        try {
            List<TicketDomainEvent> events = ticket.cancel();
            ticketRepository.save(ticket);
            domainEventPublisher.publish(ticket, events);
        } catch (TicketCannotBeCancelledException | UnsupportedStateTransitionException e) {
            domainEventPublisher.publish(ticket, List.of(new TicketCancellationRejectedEvent(orderId, e.getMessage())));
        }
    }

    @Transactional
    public void handleOrderRevisionProposed(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        Ticket ticket = ticketRepository.findByOrderId(event.orderId()).orElse(null);
        if (ticket == null) {
            return;
        }

        int newTotalQuantity = totalQuantity(event.lineItems());
        if (!isWithinCapacity(newTotalQuantity)) {
            domainEventPublisher.publish(ticket, List.of(
                    new TicketRevisionRejectedEvent(event.orderId(), "order exceeds kitchen capacity")));
            return;
        }

        List<TicketDomainEvent> events = ticket.reviseQuantity(newTotalQuantity);
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @Transactional
    public void handleOrderRevisionRejected(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(event.eventId()));

        Ticket ticket = ticketRepository.findByOrderId(event.orderId()).orElse(null);
        if (ticket == null) {
            return;
        }

        int originalTotalQuantity = totalQuantity(event.lineItems());
        List<TicketDomainEvent> events = ticket.undoRevision(originalTotalQuantity);
        ticketRepository.save(ticket);
        domainEventPublisher.publish(ticket, events);
    }

    @Transactional
    public void handleReviseTicketCommand(String eventId, Long orderId, Integer newTotalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }

        if (newTotalQuantity == null) {
            publishReply("TicketRevisionRejected", orderId, "totalQuantity is required", "ReviseOrder");
            return;
        }

        if (!isWithinCapacity(newTotalQuantity)) {
            publishReply("TicketRevisionRejected", orderId, "order exceeds kitchen capacity", "ReviseOrder");
            return;
        }

        ticket.reviseQuantity(newTotalQuantity);
        ticketRepository.save(ticket);
        publishReply("TicketQuantityRevised", orderId, null, "ReviseOrder");
    }

    @Transactional
    public void handleUndoReviseTicketCommand(String eventId, Long orderId, Integer originalTotalQuantity) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        if (originalTotalQuantity == null) {
            publishReply("TicketRevisionRejected", orderId, "originalTotalQuantity is required", "ReviseOrder");
            return;
        }

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket == null) {
            return;
        }

        ticket.undoRevision(originalTotalQuantity);
        ticketRepository.save(ticket);
        publishReply("TicketRevisionUndone", orderId, null, "ReviseOrder");
    }

    private int totalQuantity(List<OrderCreatedEvent.LineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderCreatedEvent.LineItem::quantity).sum();
    }

    private boolean isWithinCapacity(int totalQuantity) {
        return totalQuantity <= KITCHEN_CAPACITY_LIMIT;
    }

    private void publishReply(String eventType, Long orderId, String reason, String sagaType) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "kitchen", eventType, orderId, reason, sagaType);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "saga.replies", toJson(reply)));
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize saga event", e);
        }
    }
}
