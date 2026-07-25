package com.sanjay.ftgo.orderhistory.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.orderhistory.domain.OrderEvent;
import com.sanjay.ftgo.orderhistory.domain.OrderViewLineItem;
import com.sanjay.ftgo.orderhistory.domain.OrderViewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final OrderViewService orderViewService;
    private final ObjectMapper objectMapper;

    public OrderEventListener(OrderViewService orderViewService, ObjectMapper objectMapper) {
        this.orderViewService = orderViewService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.events", groupId = "order-history-service")
    public void onMessage(String payload) {
        OrderEvent event;
        try {
            event = objectMapper.readValue(payload, OrderEvent.class);
        } catch (Exception e) {
            log.warn("Skipping malformed order event: {}", payload, e);
            return;
        }
        List<OrderViewLineItem> lineItems = event.lineItems() == null ? null
                : event.lineItems().stream().map(li -> new OrderViewLineItem(li.menuItemId(), li.quantity())).toList();
        orderViewService.handleOrderEvent(event.eventId(), event.eventType(), event.orderId(),
                event.consumerId(), event.restaurantId(), lineItems);
    }
}
