package com.sanjay.ftgo.order.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

// Deliberately stateless, like CancelOrderSagaOrchestrator: Revise Order is a strict linear
// pipeline (kitchen re-check -> accounting re-authorize -> confirm/reject), and both the pending
// revised quantity and the original quantity are recomputed from Order's own line items rather
// than threaded through the reply chain, so no persisted saga instance table is needed either.
@Service
public class ReviseOrderSagaOrchestrator {

    private final OrderTransitions orderTransitions;
    private final ProcessedEventRepository processedEventRepository;
    private final SagaCommandPublisher sagaCommandPublisher;

    public ReviseOrderSagaOrchestrator(OrderTransitions orderTransitions,
                                        ProcessedEventRepository processedEventRepository,
                                        SagaCommandPublisher sagaCommandPublisher) {
        this.orderTransitions = orderTransitions;
        this.processedEventRepository = processedEventRepository;
        this.sagaCommandPublisher = sagaCommandPublisher;
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
            case "TicketRevisionRejected", "TicketRevisionUndone" ->
                    orderTransitions.rejectRevision(orderId, UUID.randomUUID().toString());
            case "TicketQuantityRevised" -> tryAuthorize(orderId);
            default -> { }
        }
    }

    private void tryAuthorize(Long orderId) {
        orderTransitions.findById(orderId).ifPresent(order -> {
            int newTotalQuantity = totalQuantity(order.getPendingRevisedLineItems());
            String eventId = UUID.randomUUID().toString();
            publishAccountingCommand(eventId, "ReviseAuthorization", orderId, newTotalQuantity);
        });
    }

    private void handleAccountingReply(String eventType, Long orderId) {
        switch (eventType) {
            case "AuthorizationRevised" -> orderTransitions.confirmRevision(orderId, UUID.randomUUID().toString());
            case "AuthorizationRevisionRejected" -> sendUndoReviseTicket(orderId);
            default -> { }
        }
    }

    private void sendUndoReviseTicket(Long orderId) {
        orderTransitions.findById(orderId).ifPresent(order -> {
            int originalTotalQuantity = totalQuantity(order.getLineItems());
            String eventId = UUID.randomUUID().toString();
            publishKitchenCommand(eventId, "UndoReviseTicket", orderId, originalTotalQuantity);
        });
    }

    private int totalQuantity(java.util.List<OrderLineItem> lineItems) {
        return lineItems.stream().mapToInt(OrderLineItem::quantity).sum();
    }

    private void publishKitchenCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        sagaCommandPublisher.publish("kitchen.commands", eventId, commandType, orderId,
                new KitchenCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"));
    }

    private void publishAccountingCommand(String eventId, String commandType, Long orderId, int totalQuantity) {
        sagaCommandPublisher.publish("accounting.commands", eventId, commandType, orderId,
                new AccountingCommand(eventId, commandType, orderId, totalQuantity, "ReviseOrder"));
    }
}
