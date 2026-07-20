package com.sanjay.ftgo.kitchen.domain;

public record TicketPreparingStartedEvent(Long orderId) implements TicketDomainEvent {
}
