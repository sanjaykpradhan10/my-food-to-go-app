package com.sanjay.ftgo.restaurant.api;

import java.math.BigDecimal;
import java.util.List;

public record CreateRestaurantRequest(String name, List<MenuItemRequest> menuItems) {

    public record MenuItemRequest(String name, BigDecimal price) {
    }
}
