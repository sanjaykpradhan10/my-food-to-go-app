package com.sanjay.ftgo.order.domain;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final RestaurantServicePort restaurantServicePort;
    private final OrderRepository orderRepository;

    public OrderService(RestaurantServicePort restaurantServicePort, OrderRepository orderRepository) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderRepository = orderRepository;
    }

    @Transactional
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

        return orderRepository.save(new Order(restaurantId, lineItems, OrderStatus.APPROVED));
    }
}
