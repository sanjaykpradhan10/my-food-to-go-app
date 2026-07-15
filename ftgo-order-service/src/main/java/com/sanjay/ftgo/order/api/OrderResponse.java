package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.Order;

import java.util.List;

public record OrderResponse(Long id, Long restaurantId, List<LineItemResponse> lineItems, String status) {

    public record LineItemResponse(Long menuItemId, int quantity) {
    }

    public static OrderResponse from(Order order) {
        List<LineItemResponse> items = order.getLineItems().stream()
                .map(lineItem -> new LineItemResponse(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
        return new OrderResponse(order.getId(), order.getRestaurantId(), items, order.getStatus().name());
    }
}
