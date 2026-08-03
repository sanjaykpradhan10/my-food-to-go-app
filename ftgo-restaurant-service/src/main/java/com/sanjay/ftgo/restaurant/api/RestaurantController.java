package com.sanjay.ftgo.restaurant.api;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import com.sanjay.ftgo.restaurant.infrastructure.RestaurantRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/restaurants")
public class RestaurantController {

    private final RestaurantRepository restaurantRepository;

    public RestaurantController(RestaurantRepository restaurantRepository) {
        this.restaurantRepository = restaurantRepository;
    }

    @GetMapping("/{id}")
    public ResponseEntity<RestaurantResponse> getRestaurant(@PathVariable Long id) {
        return restaurantRepository.findById(id)
                .map(RestaurantResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PreAuthorize("hasAnyRole('RESTAURANT', 'ADMIN')")
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public RestaurantResponse createRestaurant(@RequestBody CreateRestaurantRequest request) {
        List<MenuItem> menuItems = request.menuItems().stream()
                .map(item -> new MenuItem(item.name(), item.price()))
                .toList();
        Restaurant restaurant = restaurantRepository.save(new Restaurant(request.name(), menuItems));
        return RestaurantResponse.from(restaurant);
    }
}
