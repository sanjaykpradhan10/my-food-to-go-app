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

        int totalQuantity = event.lineItems().stream()
                .mapToInt(OrderCreatedEvent.LineItem::quantity)
                .sum();

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
            publishReply("TicketCreationFailed", orderId, "totalQuantity is required");
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            publishReply("TicketCreationFailed", orderId, "order exceeds kitchen capacity");
            return;
        }

        TicketCreationResult result = Ticket.createTicket(orderId, totalQuantity);
        ticketRepository.save(result.ticket());
        publishReply("TicketCreated", orderId, null);
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
    public void handleCancelTicketCommand(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            ticket.cancel();
            ticketRepository.save(ticket);
        }
    }

    private boolean isWithinCapacity(int totalQuantity) {
        return totalQuantity <= KITCHEN_CAPACITY_LIMIT;
    }

    private void publishReply(String eventType, Long orderId, String reason) {
        String eventId = UUID.randomUUID().toString();
        SagaReply reply = new SagaReply(eventId, "kitchen", eventType, orderId, reason);
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
