package com.sanjay.ftgo.order.domain;

import java.math.BigDecimal;
import java.util.List;

public record RestaurantInfo(Long id, String name, List<MenuItemInfo> menuItems) {

    public record MenuItemInfo(Long id, String name, BigDecimal price) {
    }
}
