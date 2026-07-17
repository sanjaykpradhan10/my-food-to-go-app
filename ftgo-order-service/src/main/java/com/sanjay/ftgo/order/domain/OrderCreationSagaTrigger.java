package com.sanjay.ftgo.order.domain;

public interface OrderCreationSagaTrigger {

    void onOrderCreated(Order order, String eventId);
}
