package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderRevisedEvent(Long orderId, List<OrderLineItem> revisedLineItems) implements OrderDomainEvent {
}
