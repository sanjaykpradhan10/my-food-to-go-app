package com.sanjay.ftgo.order.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(RestaurantServicePort restaurantServicePort,
                         OrderRepository orderRepository,
                         OutboxEventRepository outboxEventRepository,
                         ObjectMapper objectMapper) {
        this.restaurantServicePort = restaurantServicePort;
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
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

        Order order = orderRepository.save(new Order(restaurantId, lineItems, OrderStatus.APPROVED));

        String eventId = UUID.randomUUID().toString();
        OrderCreatedEvent event = OrderCreatedEvent.from(order, eventId);
        outboxEventRepository.save(new OutboxEvent(eventId, "OrderCreated", order.getId(), toJson(event)));

        return order;
    }

    private String toJson(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderCreatedEvent for order " + event.orderId(), e);
        }
    }
}
