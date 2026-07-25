package com.sanjay.ftgo.orderhistory.api;

import com.sanjay.ftgo.orderhistory.domain.OrderView;
import com.sanjay.ftgo.orderhistory.domain.OrderViewRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/order-views")
public class OrderHistoryController {

    private final OrderViewRepository orderViewRepository;

    public OrderHistoryController(OrderViewRepository orderViewRepository) {
        this.orderViewRepository = orderViewRepository;
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderViewResponse> view(@PathVariable Long orderId) {
        return orderViewRepository.findById(orderId)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    private OrderViewResponse toResponse(OrderView view) {
        return new OrderViewResponse(
                view.getOrderId(), view.getConsumerId(), view.getRestaurantId(),
                view.getOrderStatus(), view.getTicketStatus(), view.getAuthorizationStatus(),
                view.getDeliveryStatus(), view.getCourierId(),
                view.getLineItems().stream()
                        .map(li -> new OrderViewResponse.LineItemView(li.menuItemId(), li.quantity()))
                        .toList());
    }
}
