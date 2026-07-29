package com.sanjay.ftgo.orderhistory.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEvent;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class OrderViewService {

    private final OrderViewRepository orderViewRepository;
    private final ProcessedEventRepository processedEventRepository;

    public OrderViewService(OrderViewRepository orderViewRepository, ProcessedEventRepository processedEventRepository) {
        this.orderViewRepository = orderViewRepository;
        this.processedEventRepository = processedEventRepository;
    }

    // Upsert, not create-only: a ticket/authorization/delivery event can legitimately arrive
    // before this OrderCreated, since Kafka gives no ordering guarantee across topics. If a
    // stub row already exists (created by one of those), this fills in the fields OrderCreated
    // owns without disturbing whatever the stub already recorded.
    @Transactional
    public void handleOrderEvent(String eventId, String eventType, Long orderId,
                                  Long consumerId, Long restaurantId, List<OrderViewLineItem> lineItems) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

        switch (eventType) {
            case "OrderCreated" -> {
                view.setConsumerId(consumerId);
                view.setRestaurantId(restaurantId);
                view.setLineItems(lineItems);
                view.setOrderStatus("APPROVAL_PENDING");
            }
            case "OrderApproved" -> view.setOrderStatus("APPROVED");
            case "OrderRejected" -> view.setOrderStatus("REJECTED");
            case "OrderCancelled" -> view.setOrderStatus("CANCEL_PENDING");
            case "OrderCancelConfirmed" -> view.setOrderStatus("CANCELLED");
            case "OrderCancelRejected" -> view.setOrderStatus("APPROVED");
            case "OrderRevisionProposed" -> view.setOrderStatus("REVISION_PENDING");
            // The wire payload's lineItems field carries the revised line items here (see
            // OrderEventSerializer in ftgo-order-service - OrderRevisedEvent's revisedLineItems
            // is serialized onto the same generic "lineItems" wire field OrderCreated uses).
            case "OrderRevised" -> {
                view.setLineItems(lineItems);
                view.setOrderStatus("APPROVED");
            }
            case "OrderRevisionRejected" -> view.setOrderStatus("APPROVED");
            // OrderRevisionCompensationRequested: wire-only pseudo-event signalling kitchen to
            // undo a provisional revision - the order itself stays REVISION_PENDING at this
            // point, so no orderStatus change here (see Ch.6's event-sourcing design docs for
            // the full rationale behind this pseudo-event's existence).
            default -> { }
        }

        orderViewRepository.save(view);
    }

    // Same upsert pattern as handleOrderEvent, mirrored for the kitchen.events topic: kitchen
    // ticket events can likewise arrive before OrderCreated, so this must not assume a row exists.
    @Transactional
    public void handleKitchenEvent(String eventId, String eventType, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

        switch (eventType) {
            case "TicketCreated" -> view.setTicketStatus("CREATE_PENDING");
            case "TicketConfirmed" -> view.setTicketStatus("AWAITING_ACCEPTANCE");
            case "TicketAccepted" -> view.setTicketStatus("ACCEPTED");
            case "TicketPreparingStarted" -> view.setTicketStatus("PREPARING");
            case "TicketReadyForPickup" -> view.setTicketStatus("READY_FOR_PICKUP");
            case "TicketPickedUp" -> view.setTicketStatus("PICKED_UP");
            case "TicketCancelled" -> view.setTicketStatus("CANCELLED");
            // TicketCreationFailed: no ticket was ever created, nothing to record.
            // TicketCancellationRejected/TicketRevisionRejected/TicketRevisionUndone/
            // TicketQuantityRevised: none represent a new lifecycle state on their own -
            // TicketQuantityRevised changes quantity, not status, and this read model doesn't
            // track quantity at all (out of scope per the design).
            default -> { }
        }

        orderViewRepository.save(view);
    }

    // Same upsert pattern again, for accounting.events: authorization can arrive before
    // OrderCreated too, same cross-topic ordering caveat as the other two handlers.
    @Transactional
    public void handleAccountingEvent(String eventId, String eventType, Long orderId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

        switch (eventType) {
            case "CardAuthorized" -> view.setAuthorizationStatus("AUTHORIZED");
            case "CardAuthorizationFailed" -> view.setAuthorizationStatus("DECLINED");
            case "AuthorizationReversed" -> view.setAuthorizationStatus("REVERSED");
            // Still authorized, just at a new quantity - this read model doesn't track quantity.
            case "AuthorizationRevised" -> view.setAuthorizationStatus("AUTHORIZED");
            // Decline of a revision attempt, not a new authorization state - the existing
            // authorization is untouched.
            default -> { }
        }

        orderViewRepository.save(view);
    }

    // Same upsert pattern again, for delivery.events: courier assignment can arrive before
    // OrderCreated too, same cross-topic ordering caveat as the other three handlers.
    @Transactional
    public void handleDeliveryEvent(String eventId, String eventType, Long orderId, Long courierId) {
        if (processedEventRepository.existsById(eventId)) {
            return;
        }
        processedEventRepository.save(new ProcessedEvent(eventId));

        OrderView view = orderViewRepository.findById(orderId).orElseGet(() -> new OrderView(orderId));

        switch (eventType) {
            case "DeliveryScheduled" -> {
                view.setDeliveryStatus("SCHEDULED");
                view.setCourierId(courierId);
            }
            case "DeliveryPickedUp" -> view.setDeliveryStatus("PICKED_UP");
            case "DeliveryDelivered" -> view.setDeliveryStatus("DELIVERED");
            case "DeliveryCancelled" -> {
                view.setDeliveryStatus("CANCELLED");
                view.setCourierId(null); // no courier is assigned anymore
            }
            // DeliverySchedulingFailed: nothing was ever scheduled, nothing to record.
            default -> { }
        }

        orderViewRepository.save(view);
    }
}
