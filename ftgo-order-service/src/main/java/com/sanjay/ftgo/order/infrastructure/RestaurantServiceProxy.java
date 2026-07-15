package com.sanjay.ftgo.order.infrastructure;

import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServicePort;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class RestaurantServiceProxy implements RestaurantServicePort {

    private final RestClient restClient;

    public RestaurantServiceProxy(RestClient restaurantServiceRestClient) {
        this.restClient = restaurantServiceRestClient;
    }

    @Override
    @CircuitBreaker(name = "restaurantService", fallbackMethod = "findRestaurantFallback")
    public RestaurantInfo findRestaurant(Long restaurantId) {
        try {
            return restClient.get()
                    .uri("/restaurants/{id}", restaurantId)
                    .retrieve()
                    .body(RestaurantInfo.class);
        } catch (HttpClientErrorException.NotFound e) {
            throw new RestaurantNotFoundException(restaurantId);
        }
    }

    @SuppressWarnings("unused")
    private RestaurantInfo findRestaurantFallback(Long restaurantId, Throwable throwable) {
        if (throwable instanceof RestaurantNotFoundException notFound) {
            throw notFound;
        }
        throw new RestaurantServiceUnavailableException(restaurantId, throwable);
    }
}
