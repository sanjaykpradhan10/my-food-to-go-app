package com.sanjay.ftgo.order.domain;

import java.util.List;

public class Order {

    private final Long id;
    private final Long restaurantId;
    private final List<OrderLineItem> lineItems;
    private final OrderStatus status;

    public Order(Long id, Long restaurantId, List<OrderLineItem> lineItems, OrderStatus status) {
        this.id = id;
        this.restaurantId = restaurantId;
        this.lineItems = lineItems;
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public Long getRestaurantId() {
        return restaurantId;
    }

    public List<OrderLineItem> getLineItems() {
        return lineItems;
    }

    public OrderStatus getStatus() {
        return status;
    }
}
