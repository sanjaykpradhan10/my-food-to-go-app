package com.sanjay.ftgo.order.domain;

import java.util.List;

public interface OrderRevisionSagaTrigger {

    void onOrderRevised(Order order, List<OrderDomainEvent> events);
}
