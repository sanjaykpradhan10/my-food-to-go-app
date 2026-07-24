package com.sanjay.ftgo.order.domain;

import java.util.List;
import java.util.Optional;

public interface OrderTransitions {

    Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId);

    Optional<Order> findById(Long orderId);

    TransitionResult cancel(Long orderId, String eventId);

    TransitionResult revise(Long orderId, OrderRevision revision, String eventId);

    void approve(Long orderId, String eventId);

    void reject(Long orderId, String eventId);

    void noteCancelled(Long orderId, String eventId);

    void undoCancel(Long orderId, String eventId);

    void confirmRevision(Long orderId, String eventId);

    void rejectRevision(Long orderId, String eventId);

    void requestRevisionCompensation(Long orderId, String eventId);
}
