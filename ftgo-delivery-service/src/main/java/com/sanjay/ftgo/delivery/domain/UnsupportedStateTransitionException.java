package com.sanjay.ftgo.delivery.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(DeliveryStatus status) {
        super("Unsupported transition from state " + status);
    }
}
