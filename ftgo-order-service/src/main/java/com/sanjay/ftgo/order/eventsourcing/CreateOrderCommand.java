package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderLineItem;

import java.util.List;

public record CreateOrderCommand(Long orderId, Long consumerId, Long restaurantId,
                                  List<OrderLineItem> lineItems) implements OrderCommand {
}
