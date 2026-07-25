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
            case "OrderRevised" -> view.setOrderStatus("APPROVED");
            case "OrderRevisionRejected" -> view.setOrderStatus("APPROVED");
            // OrderRevisionCompensationRequested: wire-only pseudo-event signalling kitchen to
            // undo a provisional revision - the order itself stays REVISION_PENDING at this
            // point, so no orderStatus change here (see Ch.6's event-sourcing design docs for
            // the full rationale behind this pseudo-event's existence).
            default -> { }
        }

        orderViewRepository.save(view);
    }
}
