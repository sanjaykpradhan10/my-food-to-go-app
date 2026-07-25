package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.AccountingServicePort;
import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.DeliveryServicePort;
import com.sanjay.ftgo.order.domain.KitchenServicePort;
import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderRepository;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.RestaurantServicePort;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@RestController
@RequestMapping("/orders")
public class OrderViewController {

    private final OrderRepository orderRepository;
    private final RestaurantServicePort restaurantServicePort;
    private final KitchenServicePort kitchenServicePort;
    private final AccountingServicePort accountingServicePort;
    private final DeliveryServicePort deliveryServicePort;
    private final ExecutorService orderViewExecutor;

    public OrderViewController(OrderRepository orderRepository,
                                RestaurantServicePort restaurantServicePort,
                                KitchenServicePort kitchenServicePort,
                                AccountingServicePort accountingServicePort,
                                DeliveryServicePort deliveryServicePort,
                                ExecutorService orderViewExecutor) {
        this.orderRepository = orderRepository;
        this.restaurantServicePort = restaurantServicePort;
        this.kitchenServicePort = kitchenServicePort;
        this.accountingServicePort = accountingServicePort;
        this.deliveryServicePort = deliveryServicePort;
        this.orderViewExecutor = orderViewExecutor;
    }

    @GetMapping("/{id}/view")
    public ResponseEntity<OrderViewResponse> view(@PathVariable Long id) {
        Order order = orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));

        // Fire all 4 downstream section lookups concurrently on virtual threads - each is an
        // independent, degradable section (see SectionResult), so none should block the others.
        CompletableFuture<SectionResult<RestaurantInfo>> restaurantFuture =
                CompletableFuture.supplyAsync(() -> restaurantServicePort.findRestaurantForView(order.getRestaurantId()), orderViewExecutor);
        CompletableFuture<SectionResult<TicketInfo>> ticketFuture =
                CompletableFuture.supplyAsync(() -> kitchenServicePort.findTicket(id), orderViewExecutor);
        CompletableFuture<SectionResult<AuthorizationInfo>> authorizationFuture =
                CompletableFuture.supplyAsync(() -> accountingServicePort.findAuthorization(id), orderViewExecutor);
        CompletableFuture<SectionResult<DeliveryInfo>> deliveryFuture =
                CompletableFuture.supplyAsync(() -> deliveryServicePort.findDelivery(id), orderViewExecutor);

        CompletableFuture.allOf(restaurantFuture, ticketFuture, authorizationFuture, deliveryFuture).join();

        OrderSummary summary = new OrderSummary(
                order.getId(),
                order.getStatus().name(),
                order.getConsumerId(),
                order.getRestaurantId(),
                order.getLineItems().stream()
                        .map(item -> new OrderSummary.LineItemView(item.menuItemId(), item.quantity()))
                        .toList());

        OrderViewResponse response = new OrderViewResponse(
                summary, restaurantFuture.join(), ticketFuture.join(), authorizationFuture.join(), deliveryFuture.join());
        return ResponseEntity.ok(response);
    }

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<String> handleNotFound(OrderNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }
}
