package com.sanjay.ftgo.order.domain;

public sealed interface OrderDomainEvent
        permits OrderApprovedEvent, OrderRejectedEvent, OrderCancelledEvent, OrderCancelConfirmedEvent,
                OrderCancelRejectedEvent, OrderRevisionProposedEvent, OrderRevisedEvent, OrderRevisionRejectedEvent {

    Long orderId();
}
