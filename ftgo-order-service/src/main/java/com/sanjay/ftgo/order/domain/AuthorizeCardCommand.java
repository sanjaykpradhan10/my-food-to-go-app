package com.sanjay.ftgo.order.domain;

public record AuthorizeCardCommand(String eventId, Long orderId, Integer totalQuantity) {
}
