package com.sanjay.ftgo.order.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(OrderStatus status) {
        super("Unsupported transition from status " + status);
    }
}
