package com.sanjay.ftgo.order.domain;

public record KitchenEvent(
        String eventId,
        String eventType,
        Long orderId,
        Long ticketId,
        Integer totalQuantity,
        String reason) {
}
