package com.sanjay.ftgo.kitchen.domain;

public class TicketNotFoundException extends RuntimeException {

    public TicketNotFoundException(Long ticketId) {
        super("Ticket not found: " + ticketId);
    }
}
