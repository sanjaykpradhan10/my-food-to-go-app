package com.sanjay.ftgo.restaurant.api;

import com.sanjay.ftgo.restaurant.domain.MenuItem;
import com.sanjay.ftgo.restaurant.domain.Restaurant;
import com.sanjay.ftgo.restaurant.infrastructure.RestaurantRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
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

    @Test
    void createsRestaurantWithMenuItems() throws Exception {
        Restaurant saved = new Restaurant("Ajanta E2E", List.of(
                new MenuItem("Chicken Vindaloo", new BigDecimal("12.00"))
        ));
        setId(saved, 5L);
        setId(saved.getMenuItems().get(0), 50L);
        when(restaurantRepository.save(any(Restaurant.class))).thenReturn(saved);

        mockMvc.perform(post("/restaurants")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name":"Ajanta E2E","menuItems":[{"name":"Chicken Vindaloo","price":12.00}]}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(5))
                .andExpect(jsonPath("$.name").value("Ajanta E2E"))
                .andExpect(jsonPath("$.menuItems[0].id").value(50))
                .andExpect(jsonPath("$.menuItems[0].name").value("Chicken Vindaloo"))
                .andExpect(jsonPath("$.menuItems[0].price").value(12.00));
    }

    @Test
    void passesNameAndMenuItemsToRepository() throws Exception {
        Restaurant saved = new Restaurant("Pizza E2E", List.of(new MenuItem("Slice", new BigDecimal("3.00"))));
        setId(saved, 6L);
        setId(saved.getMenuItems().get(0), 60L);
        when(restaurantRepository.save(any(Restaurant.class))).thenReturn(saved);

        mockMvc.perform(post("/restaurants")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"name":"Pizza E2E","menuItems":[{"name":"Slice","price":3.00}]}
                                """))
                .andExpect(status().isCreated());

        ArgumentCaptor<Restaurant> captor = ArgumentCaptor.forClass(Restaurant.class);
        verify(restaurantRepository).save(captor.capture());
        assertThat(captor.getValue().getName()).isEqualTo("Pizza E2E");
        assertThat(captor.getValue().getMenuItems()).extracting(MenuItem::getName).containsExactly("Slice");
    }

    private static void setId(Object entity, Long id) throws Exception {
        Field idField = entity.getClass().getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(entity, id);
    }
}
