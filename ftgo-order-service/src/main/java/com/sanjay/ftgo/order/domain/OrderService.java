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
    private final OrderRepository orderRepository;
    private final OrderCreationSagaTrigger orderCreationSagaTrigger;

    public OrderService(RestaurantServicePort restaurantServicePort,
                         OrderRepository orderRepository,
                         OrderCreationSagaTrigger orderCreationSagaTrigger) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderRepository = orderRepository;
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

        Order order = orderRepository.save(new Order(consumerId, restaurantId, lineItems, OrderStatus.APPROVAL_PENDING));

        String eventId = UUID.randomUUID().toString();
        orderCreationSagaTrigger.onOrderCreated(order, eventId);

        return order;
    }
}
