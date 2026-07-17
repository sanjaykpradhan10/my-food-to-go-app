package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderServiceTest {

    private final RestaurantInfo restaurant = new RestaurantInfo(1L, "Ajanta Indian Cuisine", List.of(
            new RestaurantInfo.MenuItemInfo(10L, "Chicken Tikka Masala", new BigDecimal("14.99")),
            new RestaurantInfo.MenuItemInfo(11L, "Garlic Naan", new BigDecimal("3.50"))
    ));

    private final RestaurantServicePort fakePort = restaurantId ->
            restaurantId.equals(1L) ? restaurant : null;

    private final OrderRepository orderRepository = mock(OrderRepository.class);
    private final OrderCreationSagaTrigger orderCreationSagaTrigger = mock(OrderCreationSagaTrigger.class);

    private final OrderService orderService =
            new OrderService(fakePort, orderRepository, orderCreationSagaTrigger);

    @Test
    void createsOrderInApprovalPendingWhenRestaurantAndMenuItemsAreValid() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        Order order = orderService.createOrder(1L, 1L, List.of(new OrderLineItem(10L, 2)));

        assertThat(order.getConsumerId()).isEqualTo(1L);
        assertThat(order.getRestaurantId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
        assertThat(order.getLineItems()).containsExactly(new OrderLineItem(10L, 2));
    }

    @Test
    void triggersSagaWhenOrderIsCreated() {
        when(orderRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));

        orderService.createOrder(1L, 1L, List.of(new OrderLineItem(10L, 2)));

        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        ArgumentCaptor<String> eventIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(orderCreationSagaTrigger).onOrderCreated(orderCaptor.capture(), eventIdCaptor.capture());
        assertThat(orderCaptor.getValue().getRestaurantId()).isEqualTo(1L);
        assertThat(eventIdCaptor.getValue()).isNotBlank();
    }

    @Test
    void rejectsOrderWhenMenuItemDoesNotBelongToRestaurant() {
        assertThatThrownBy(() -> orderService.createOrder(1L, 1L, List.of(new OrderLineItem(999L, 1))))
                .isInstanceOf(MenuItemNotFoundException.class);
    }
}
