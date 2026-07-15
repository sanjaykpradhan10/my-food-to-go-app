package com.sanjay.ftgo.order.domain;

public class RestaurantNotFoundException extends RuntimeException {

    public RestaurantNotFoundException(Long restaurantId) {
        super("Restaurant not found: " + restaurantId);
    }
}
