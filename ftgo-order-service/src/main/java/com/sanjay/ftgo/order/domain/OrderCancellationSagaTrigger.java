package com.sanjay.ftgo.order.domain;

import java.util.List;

public interface OrderCancellationSagaTrigger {

    void onOrderCancelled(Order order, List<OrderDomainEvent> events);
}
