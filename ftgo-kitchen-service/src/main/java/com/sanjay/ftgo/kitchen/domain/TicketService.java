package com.sanjay.ftgo.kitchen.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

        if (totalQuantity > KITCHEN_CAPACITY_LIMIT) {
            publishEvent("TicketCreationFailed", event.orderId(), null, totalQuantity,
                    "order exceeds kitchen capacity");
            return;
        }

        Ticket ticket = ticketRepository.save(new Ticket(event.orderId(), "CREATE_PENDING"));
        publishEvent("TicketCreated", event.orderId(), ticket.getId(), totalQuantity, null);
    }

    private void publishEvent(String eventType, Long orderId, Long ticketId, Integer totalQuantity, String reason) {
        String eventId = UUID.randomUUID().toString();
        KitchenEvent event = new KitchenEvent(eventId, eventType, orderId, ticketId, totalQuantity, reason);
        outboxEventRepository.save(new OutboxEvent(eventId, eventType, orderId, toJson(event)));
    }

    private String toJson(KitchenEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + event.eventType() + " for order " + event.orderId(), e);
        }
    }
}
