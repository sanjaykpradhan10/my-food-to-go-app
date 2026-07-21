package com.sanjay.ftgo.order.domain;

public record OrderRejectedEvent(Long orderId) implements OrderDomainEvent {
}
