package com.sanjay.ftgo.kitchen.domain;

import java.time.ZonedDateTime;

public record TicketAcceptedEvent(Long orderId, ZonedDateTime readyBy) implements TicketDomainEvent {
}
