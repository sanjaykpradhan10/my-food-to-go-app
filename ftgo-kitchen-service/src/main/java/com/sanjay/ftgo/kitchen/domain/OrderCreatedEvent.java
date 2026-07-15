package com.sanjay.ftgo.kitchen.domain;

import java.util.List;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long restaurantId,
        List<LineItem> lineItems) {

    public record LineItem(Long menuItemId, int quantity) {
    }
}
