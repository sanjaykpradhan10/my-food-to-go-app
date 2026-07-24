package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderCreatedEvent(Long orderId, Long consumerId, Long restaurantId,
                                 List<OrderLineItem> lineItems) implements OrderDomainEvent {
}
