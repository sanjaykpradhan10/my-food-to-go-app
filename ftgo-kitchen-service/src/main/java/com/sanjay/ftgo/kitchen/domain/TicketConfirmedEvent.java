package com.sanjay.ftgo.kitchen.domain;

public record TicketConfirmedEvent(Long orderId) implements TicketDomainEvent {
}
