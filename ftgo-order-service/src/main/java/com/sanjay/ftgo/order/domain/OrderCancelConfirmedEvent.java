package com.sanjay.ftgo.order.domain;

public record OrderCancelConfirmedEvent(Long orderId) implements OrderDomainEvent {
}
