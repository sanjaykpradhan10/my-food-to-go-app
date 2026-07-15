package com.sanjay.ftgo.order.domain;

public class RestaurantServiceUnavailableException extends RuntimeException {

    public RestaurantServiceUnavailableException(Long restaurantId, Throwable cause) {
        super("Restaurant service unavailable while looking up restaurant: " + restaurantId, cause);
    }
}
