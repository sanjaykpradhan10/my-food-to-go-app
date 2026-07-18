package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEvent;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class TicketService {

    private static final int KITCHEN_CAPACITY_LIMIT = 20;

    private final TicketRepository ticketRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final FailedOrderRepository failedOrderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public TicketService(TicketRepository ticketRepository,
                          ProcessedEventRepository processedEventRepository,
                          FailedOrderRepository failedOrderRepository,
                          OutboxEventRepository outboxEventRepository,
                          ObjectMapper objectMapper) {
        this.ticketRepository = ticketRepository;
        this.processedEventRepository = processedEventRepository;
        this.failedOrderRepository = failedOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
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
            ticketRepository.save(new Ticket(event.orderId(), "CANCELLED"));
            return;
        }

        if (!isWithinCapacity(totalQuantity)) {
            publishEvent("TicketCreationFailed", event.orderId(), null, totalQuantity,
                    "order exceeds kitchen capacity");
            return;
        }

        Ticket ticket = ticketRepository.save(new Ticket(event.orderId(), "CREATE_PENDING"));
        publishEvent("TicketCreated", event.orderId(), ticket.getId(), totalQuantity, null);
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

        if ("CardAuthorized".equals(eventType)) {
            ticket.markAwaitingAcceptance();
            ticketRepository.save(ticket);
            publishEvent("TicketConfirmed", orderId, ticket.getId(), null, null);
        } else {
            ticket.markCancelled();
            ticketRepository.save(ticket);
            publishEvent("TicketCancelled", orderId, ticket.getId(), null, null);
        }
    }

    @Transactional
    public void handleConsumerVerificationFailed(String eventId, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        Ticket ticket = ticketRepository.findByOrderId(orderId).orElse(null);
        if (ticket != null) {
            ticket.markCancelled();
            ticketRepository.save(ticket);
            publishEvent("TicketCancelled", orderId, ticket.getId(), null, null);
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

        ticketRepository.save(new Ticket(orderId, "CREATE_PENDING"));
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
            ticket.markAwaitingAcceptance();
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
            ticket.markCancelled();
            ticketRepository.save(ticket);
        }
    }

    private boolean isWithinCapacity(int totalQuantity) {
        return totalQuantity <= KITCHEN_CAPACITY_LIMIT;
    }

    private void publishEvent(String eventType, Long orderId, Long ticketId, Integer totalQuantity, String reason) {
        String eventId = UUID.randomUUID().toString();
        KitchenEvent event = new KitchenEvent(eventId, eventType, orderId, ticketId, totalQuantity, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, "kitchen.events", toJson(event)));
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
