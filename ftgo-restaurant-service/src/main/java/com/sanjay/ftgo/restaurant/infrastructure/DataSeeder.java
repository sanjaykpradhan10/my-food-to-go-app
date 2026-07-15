package com.sanjay.ftgo.restaurant.infrastructure;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;

@Component
public class DataSeeder implements CommandLineRunner {

    private final RestaurantRepository restaurantRepository;

    public DataSeeder(RestaurantRepository restaurantRepository) {
        this.restaurantRepository = restaurantRepository;
    }

    @Override
    public void run(String... args) {
        if (restaurantRepository.count() > 0) {
            return;
        }
        restaurantRepository.save(new Restaurant("Ajanta Indian Cuisine", List.of(
                new MenuItem("Chicken Tikka Masala", new BigDecimal("14.99")),
                new MenuItem("Garlic Naan", new BigDecimal("3.50"))
        )));
        restaurantRepository.save(new Restaurant("Pizza Palace", List.of(
                new MenuItem("Margherita Pizza", new BigDecimal("12.00")),
                new MenuItem("Pepperoni Pizza", new BigDecimal("13.50"))
        )));
    }
}
