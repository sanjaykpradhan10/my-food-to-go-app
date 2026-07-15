package com.sanjay.ftgo.order;

import com.sanjay.ftgo.order.domain.RestaurantServicePort;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class TestConfig {

    @Bean
    @Primary
    public RestaurantServicePort restaurantServicePort() {
        return restaurantId -> null;
    }
}
