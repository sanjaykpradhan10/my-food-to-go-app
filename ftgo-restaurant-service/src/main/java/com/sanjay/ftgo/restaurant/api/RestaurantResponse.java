package com.sanjay.ftgo.restaurant.api;

import com.sanjay.ftgo.restaurant.domain.Restaurant;

import java.math.BigDecimal;
import java.util.List;

public record RestaurantResponse(Long id, String name, List<MenuItemResponse> menuItems) {

    public record MenuItemResponse(Long id, String name, BigDecimal price) {
    }

    public static RestaurantResponse from(Restaurant restaurant) {
        List<MenuItemResponse> items = restaurant.getMenuItems().stream()
                .map(menuItem -> new MenuItemResponse(menuItem.getId(), menuItem.getName(), menuItem.getPrice()))
                .toList();
        return new RestaurantResponse(restaurant.getId(), restaurant.getName(), items);
    }
}
