package com.sanjay.ftgo.order.domain;

public record DeliveryCommand(String eventId, String commandType, Long orderId, Long restaurantId, String sagaType) {
}
