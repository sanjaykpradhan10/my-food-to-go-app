package com.sanjay.ftgo.kitchen.domain;

import java.util.List;

public record TicketCreationResult(Ticket ticket, List<TicketDomainEvent> events) {
}
