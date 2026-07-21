package com.sanjay.ftgo.order.domain;

public record OrderCancelRejectedEvent(Long orderId) implements OrderDomainEvent {
}
