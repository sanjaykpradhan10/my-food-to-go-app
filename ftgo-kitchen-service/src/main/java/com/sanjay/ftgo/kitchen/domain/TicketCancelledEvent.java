package com.sanjay.ftgo.kitchen.domain;

public record TicketCancelledEvent(Long orderId) implements TicketDomainEvent {
}
