package com.sanjay.ftgo.kitchen.domain;

public record TicketPickedUpEvent(Long orderId) implements TicketDomainEvent {
}
