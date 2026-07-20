package com.sanjay.ftgo.kitchen.domain;

public record TicketCreationFailedEvent(Long orderId, String reason) implements TicketDomainEvent {
}
