package com.sanjay.ftgo.kitchen.domain;

public record TicketCreatedEvent(Long orderId, int totalQuantity) implements TicketDomainEvent {
}
