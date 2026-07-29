package com.sanjay.ftgo.orderhistory.api;

import java.util.List;

public record OrderViewResponse(
        Long orderId,
        Long consumerId,
        Long restaurantId,
        String orderStatus,
        String ticketStatus,
        String authorizationStatus,
        String deliveryStatus,
        Long courierId,
        List<LineItemView> lineItems) {

    public record LineItemView(Long menuItemId, int quantity) {
    }
}
