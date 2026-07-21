package com.sanjay.ftgo.order.domain;

public record OrderCancelledEvent(Long orderId) implements OrderDomainEvent {
}
