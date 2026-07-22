package com.sanjay.ftgo.kitchen.domain;

public record TicketRevisionRejectedEvent(Long orderId, String reason) implements TicketDomainEvent {
}
