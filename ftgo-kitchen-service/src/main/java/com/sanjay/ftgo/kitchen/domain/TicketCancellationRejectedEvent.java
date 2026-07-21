package com.sanjay.ftgo.kitchen.domain;

public record TicketCancellationRejectedEvent(Long orderId, String reason) implements TicketDomainEvent {
}
