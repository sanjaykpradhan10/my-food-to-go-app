package com.sanjay.ftgo.kitchen.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(TicketState state) {
        super("Unsupported transition from state " + state);
    }
}
