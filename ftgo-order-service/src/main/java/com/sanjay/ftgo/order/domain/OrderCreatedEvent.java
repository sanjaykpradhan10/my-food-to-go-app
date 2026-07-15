package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long restaurantId,
        List<LineItem> lineItems) {

    public record LineItem(Long menuItemId, int quantity) {
    }

    public static OrderCreatedEvent from(Order order, String eventId) {
        List<LineItem> items = order.getLineItems().stream()
                .map(lineItem -> new LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
        return new OrderCreatedEvent(eventId, "OrderCreated", order.getId(), order.getRestaurantId(), items);
    }
}
