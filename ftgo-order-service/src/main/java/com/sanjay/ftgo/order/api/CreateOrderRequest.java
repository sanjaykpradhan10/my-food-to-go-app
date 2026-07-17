package com.sanjay.ftgo.order.api;

import java.util.List;

public record CreateOrderRequest(Long consumerId, Long restaurantId, List<LineItemRequest> lineItems) {

    public record LineItemRequest(Long menuItemId, int quantity) {
    }
}
