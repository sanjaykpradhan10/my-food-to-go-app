package com.sanjay.ftgo.kitchen.domain;

public record TicketRevisionUndoneEvent(Long orderId, int totalQuantity) implements TicketDomainEvent {
}
