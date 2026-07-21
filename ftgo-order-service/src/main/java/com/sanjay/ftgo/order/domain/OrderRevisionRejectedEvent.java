package com.sanjay.ftgo.order.domain;

public record OrderRevisionRejectedEvent(Long orderId) implements OrderDomainEvent {
}
