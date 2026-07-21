package com.sanjay.ftgo.order.api;

import java.util.List;

public record ReviseOrderRequest(List<LineItemRequest> lineItems) {

    public record LineItemRequest(Long menuItemId, int quantity) {
    }
}
