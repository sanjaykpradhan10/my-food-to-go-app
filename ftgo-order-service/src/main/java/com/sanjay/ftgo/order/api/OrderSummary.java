package com.sanjay.ftgo.order.api;

import java.util.List;

public record OrderSummary(Long id, String status, Long consumerId, Long restaurantId, List<LineItemView> lineItems) {

    public record LineItemView(Long menuItemId, int quantity) {
    }
}
