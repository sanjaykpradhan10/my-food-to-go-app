package com.sanjay.ftgo.kitchen.domain;

public record TicketQuantityRevisedEvent(Long orderId, int totalQuantity) implements TicketDomainEvent {
}
