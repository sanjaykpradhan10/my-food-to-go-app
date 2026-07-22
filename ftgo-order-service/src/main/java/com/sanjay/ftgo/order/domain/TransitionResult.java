package com.sanjay.ftgo.order.domain;

import java.util.List;

public record TransitionResult(Order order, List<OrderDomainEvent> events) {
}
