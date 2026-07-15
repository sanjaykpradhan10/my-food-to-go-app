package com.sanjay.ftgo.order.domain;

public class MenuItemNotFoundException extends RuntimeException {

    public MenuItemNotFoundException(Long menuItemId, Long restaurantId) {
        super("Menu item " + menuItemId + " not found for restaurant " + restaurantId);
    }
}
