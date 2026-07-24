package com.sanjay.ftgo.order.domain;

public interface RestaurantServicePort {

    RestaurantInfo findRestaurant(Long restaurantId);

    SectionResult<RestaurantInfo> findRestaurantForView(Long restaurantId);
}
