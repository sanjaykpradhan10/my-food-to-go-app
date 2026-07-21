package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long consumerId,
        Long restaurantId,
        List<LineItem> lineItems) {

    public record LineItem(Long menuItemId, int quantity) {
    }
}
