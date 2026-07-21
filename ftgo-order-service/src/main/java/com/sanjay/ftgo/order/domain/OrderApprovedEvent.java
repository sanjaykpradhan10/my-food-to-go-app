package com.sanjay.ftgo.order.domain;

public record OrderApprovedEvent(Long orderId) implements OrderDomainEvent {
}
