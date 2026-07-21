package com.sanjay.ftgo.order.domain;

public class OrderCannotBeCancelledException extends RuntimeException {

    public OrderCannotBeCancelledException(Long orderId) {
        super("Order " + orderId + " cannot be cancelled in its current state");
    }
}
