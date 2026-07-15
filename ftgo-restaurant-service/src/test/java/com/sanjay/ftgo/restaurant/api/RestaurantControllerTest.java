package com.sanjay.ftgo.restaurant.api;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import com.sanjay.ftgo.restaurant.infrastructure.RestaurantRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RestaurantController.class)
class RestaurantControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RestaurantRepository restaurantRepository;

    @Test
    void returnsRestaurantWhenFound() throws Exception {
        MenuItem menuItem = new MenuItem("Chicken Tikka Masala", new BigDecimal("14.99"));
        setId(menuItem, 10L);
        Restaurant restaurant = new Restaurant("Ajanta Indian Cuisine", List.of(menuItem));
        setId(restaurant, 1L);

        when(restaurantRepository.findById(1L)).thenReturn(Optional.of(restaurant));

        mockMvc.perform(get("/restaurants/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.name").value("Ajanta Indian Cuisine"))
                .andExpect(jsonPath("$.menuItems[0].id").value(10))
                .andExpect(jsonPath("$.menuItems[0].name").value("Chicken Tikka Masala"))
                .andExpect(jsonPath("$.menuItems[0].price").value(14.99));
    }

    @Test
    void returns404WhenNotFound() throws Exception {
        when(restaurantRepository.findById(99L)).thenReturn(Optional.empty());

        mockMvc.perform(get("/restaurants/99"))
                .andExpect(status().isNotFound());
    }

    private static void setId(Object entity, Long id) throws Exception {
        Field idField = entity.getClass().getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(entity, id);
    }
}
