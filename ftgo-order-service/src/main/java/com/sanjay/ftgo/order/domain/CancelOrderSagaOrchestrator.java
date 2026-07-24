package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// Deliberately stateless, unlike CreateOrderSagaOrchestrator: Cancel Order is a strict
// linear pipeline (kitchen cancel -> accounting reversal -> order cancelled) with no
// parallel replies to join, so there's no need for a persisted saga instance table.
@Service
public class CancelOrderSagaOrchestrator {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public CancelOrderSagaOrchestrator(OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
    }

    @Transactional
    public void start(Order order) {
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("kitchen.commands", eventId, "CancelTicket", order.getId(),
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
            orderTransitions.undoCancel(orderId, UUID.randomUUID().toString());
            return;
        }
        String eventId = UUID.randomUUID().toString();
        sagaCommandPublisher.publish("accounting.commands", eventId, "ReverseAuthorization", orderId,
                new AccountingCommand(eventId, "ReverseAuthorization", orderId, null, "CancelOrder"));
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        if ("AuthorizationReversed".equals(eventType)) {
            orderTransitions.noteCancelled(orderId, UUID.randomUUID().toString());
        }
    }
}
