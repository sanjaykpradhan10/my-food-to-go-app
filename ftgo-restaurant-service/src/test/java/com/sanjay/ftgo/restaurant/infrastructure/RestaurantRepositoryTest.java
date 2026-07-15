package com.sanjay.ftgo.restaurant.infrastructure;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
class RestaurantRepositoryTest {

    @Autowired
    private RestaurantRepository restaurantRepository;

    @Test
    void savesAndFindsRestaurantWithMenuItems() {
        Restaurant restaurant = new Restaurant("Ajanta Indian Cuisine", List.of(
                new MenuItem("Chicken Tikka Masala", new BigDecimal("14.99")),
                new MenuItem("Garlic Naan", new BigDecimal("3.50"))
        ));

        Restaurant saved = restaurantRepository.save(restaurant);

        Optional<Restaurant> found = restaurantRepository.findById(saved.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getName()).isEqualTo("Ajanta Indian Cuisine");
        assertThat(found.get().getMenuItems()).hasSize(2);
        assertThat(found.get().getMenuItems())
                .extracting(MenuItem::getName)
                .containsExactlyInAnyOrder("Chicken Tikka Masala", "Garlic Naan");
    }
}
