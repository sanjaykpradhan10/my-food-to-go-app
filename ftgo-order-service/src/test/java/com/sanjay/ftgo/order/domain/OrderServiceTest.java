package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderServiceTest {

    private final RestaurantInfo restaurant = new RestaurantInfo(1L, "Ajanta Indian Cuisine", List.of(
            new RestaurantInfo.MenuItemInfo(10L, "Chicken Tikka Masala", new BigDecimal("14.99")),
            new RestaurantInfo.MenuItemInfo(11L, "Garlic Naan", new BigDecimal("3.50"))
    ));

    private final RestaurantServicePort fakePort = restaurantId ->
            restaurantId.equals(1L) ? restaurant : null;

    private final OrderService orderService = new OrderService(fakePort);

    @Test
    void createsOrderWhenRestaurantAndMenuItemsAreValid() {
        Order order = orderService.createOrder(1L, List.of(new OrderLineItem(10L, 2)));

        assertThat(order.getRestaurantId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
    }

    @Test
    void rejectsOrderWhenMenuItemDoesNotBelongToRestaurant() {
        assertThatThrownBy(() -> orderService.createOrder(1L, List.of(new OrderLineItem(999L, 1))))
                .isInstanceOf(MenuItemNotFoundException.class);
    }
}
