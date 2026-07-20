package com.sanjay.ftgo.kitchen.domain;

public class TicketCannotBeCancelledException extends RuntimeException {

    public TicketCannotBeCancelledException(Long orderId) {
        super("Ticket for order " + orderId + " cannot be cancelled once ready for pickup");
    }
}
