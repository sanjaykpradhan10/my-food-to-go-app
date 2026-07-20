package com.sanjay.ftgo.kitchen.domain;

public record TicketReadyForPickupEvent(Long orderId) implements TicketDomainEvent {
}
