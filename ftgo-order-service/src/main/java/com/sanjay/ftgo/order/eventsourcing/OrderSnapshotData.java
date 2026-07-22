package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderStatus;

import java.util.List;

public record OrderSnapshotData(Long id, Long consumerId, Long restaurantId, List<OrderLineItem> lineItems,
                                 OrderStatus status, List<OrderLineItem> pendingRevisedLineItems) {
}
