package com.sanjay.ftgo.order.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final RestaurantServicePort restaurantServicePort;
    private final OrderTransitions orderTransitions;
    private final OrderCreationSagaTrigger orderCreationSagaTrigger;

    public OrderService(RestaurantServicePort restaurantServicePort,
                         OrderTransitions orderTransitions,
                         OrderCreationSagaTrigger orderCreationSagaTrigger) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderTransitions = orderTransitions;
        this.orderCreationSagaTrigger = orderCreationSagaTrigger;
    }

    @Transactional
    public Order createOrder(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems) {
        RestaurantInfo restaurant = restaurantServicePort.findRestaurant(restaurantId);

        Set<Long> validMenuItemIds = restaurant.menuItems().stream()
                .map(RestaurantInfo.MenuItemInfo::id)
                .collect(Collectors.toSet());

        for (OrderLineItem lineItem : lineItems) {
            if (!validMenuItemIds.contains(lineItem.menuItemId())) {
                throw new MenuItemNotFoundException(lineItem.menuItemId(), restaurantId);
            }
        }

        String eventId = UUID.randomUUID().toString();
        Order order = orderTransitions.create(consumerId, restaurantId, lineItems, eventId);

        orderCreationSagaTrigger.onOrderCreated(order, eventId);

        return order;
    }
}
