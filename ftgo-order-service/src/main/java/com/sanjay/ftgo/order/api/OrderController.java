package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEventPublisher;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;
    private final OrderDomainEventPublisher domainEventPublisher;

    public OrderController(OrderService orderService, OrderRepository orderRepository,
                            OrderDomainEventPublisher domainEventPublisher) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.domainEventPublisher = domainEventPublisher;
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest request) {
        if (request.consumerId() == null) {
            return ResponseEntity.badRequest().body("consumerId is required");
        }
        if (request.restaurantId() == null) {
            return ResponseEntity.badRequest().body("restaurantId is required");
        }
        if (request.lineItems() == null || request.lineItems().isEmpty()) {
            return ResponseEntity.badRequest().body("lineItems must not be empty");
        }

        List<OrderLineItem> lineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();

        Order order = orderService.createOrder(request.consumerId(), request.restaurantId(), lineItems);
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.from(order));
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<OrderResponse> cancel(@PathVariable Long id) {
        Order order = findOrder(id);
        apply(order, order.cancel());
        return ResponseEntity.ok(OrderResponse.from(order));
    }

    @PostMapping("/{id}/revise")
    public ResponseEntity<OrderResponse> revise(@PathVariable Long id, @RequestBody ReviseOrderRequest request) {
        Order order = findOrder(id);
        List<OrderLineItem> revisedLineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();
        apply(order, order.revise(new OrderRevision(revisedLineItems)));
        return ResponseEntity.ok(OrderResponse.from(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    private void apply(Order order, List<OrderDomainEvent> events) {
        orderRepository.save(order);
        domainEventPublisher.publish(events);
    }

    @ExceptionHandler({RestaurantNotFoundException.class, MenuItemNotFoundException.class, OrderNotFoundException.class})
    public ResponseEntity<String> handleNotFound(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }

    @ExceptionHandler(RestaurantServiceUnavailableException.class)
    public ResponseEntity<String> handleUnavailable(RestaurantServiceUnavailableException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ex.getMessage());
    }

    @ExceptionHandler({OrderCannotBeCancelledException.class, UnsupportedStateTransitionException.class})
    public ResponseEntity<String> handleConflict(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ex.getMessage());
    }
}
