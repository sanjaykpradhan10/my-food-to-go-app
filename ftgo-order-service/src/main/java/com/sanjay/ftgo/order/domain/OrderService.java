package com.sanjay.ftgo.order.domain;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final RestaurantServicePort restaurantServicePort;
    private final AtomicLong idGenerator = new AtomicLong(1);

    public OrderService(RestaurantServicePort restaurantServicePort) {
        this.restaurantServicePort = restaurantServicePort;
    }

    public Order createOrder(Long restaurantId, List<OrderLineItem> lineItems) {
        RestaurantInfo restaurant = restaurantServicePort.findRestaurant(restaurantId);

        Set<Long> validMenuItemIds = restaurant.menuItems().stream()
                .map(RestaurantInfo.MenuItemInfo::id)
                .collect(Collectors.toSet());

        for (OrderLineItem lineItem : lineItems) {
            if (!validMenuItemIds.contains(lineItem.menuItemId())) {
                throw new MenuItemNotFoundException(lineItem.menuItemId(), restaurantId);
            }
        }

        return new Order(idGenerator.getAndIncrement(), restaurantId, lineItems, OrderStatus.APPROVED);
    }
}
