package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.MenuItemNotFoundException;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException;
import com.sanjay.ftgo.order.domain.OrderCancellationSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderRevisionSagaTrigger;
import com.sanjay.ftgo.order.domain.OrderService;
import com.sanjay.ftgo.order.domain.OrderTransitions;
import com.sanjay.ftgo.order.domain.RestaurantNotFoundException;
import com.sanjay.ftgo.order.domain.RestaurantServiceUnavailableException;
import com.sanjay.ftgo.order.domain.TransitionResult;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderTransitions orderTransitions;
    private final OrderCancellationSagaTrigger cancellationSagaTrigger;
    private final OrderRevisionSagaTrigger revisionSagaTrigger;

    public OrderController(OrderService orderService, OrderTransitions orderTransitions,
                            OrderCancellationSagaTrigger cancellationSagaTrigger,
                            OrderRevisionSagaTrigger revisionSagaTrigger) {
        this.orderService = orderService;
        this.orderTransitions = orderTransitions;
        this.cancellationSagaTrigger = cancellationSagaTrigger;
        this.revisionSagaTrigger = revisionSagaTrigger;
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
    @Transactional
    public ResponseEntity<OrderResponse> cancel(@PathVariable Long id) {
        TransitionResult result = orderTransitions.cancel(id, UUID.randomUUID().toString());
        cancellationSagaTrigger.onOrderCancelled(result.order(), result.events());
        return ResponseEntity.ok(OrderResponse.from(result.order()));
    }

    @PostMapping("/{id}/revise")
    @Transactional
    public ResponseEntity<OrderResponse> revise(@PathVariable Long id, @RequestBody ReviseOrderRequest request) {
        List<OrderLineItem> revisedLineItems = request.lineItems().stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();
        TransitionResult result =
                orderTransitions.revise(id, new OrderRevision(revisedLineItems), UUID.randomUUID().toString());
        revisionSagaTrigger.onOrderRevised(result.order(), result.events());
        return ResponseEntity.ok(OrderResponse.from(result.order()));
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
