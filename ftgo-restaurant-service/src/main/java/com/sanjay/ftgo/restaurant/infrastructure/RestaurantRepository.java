package com.sanjay.ftgo.restaurant.infrastructure;

import com.sanjay.ftgo.restaurant.domain.Restaurant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RestaurantRepository extends JpaRepository<Restaurant, Long> {
}
