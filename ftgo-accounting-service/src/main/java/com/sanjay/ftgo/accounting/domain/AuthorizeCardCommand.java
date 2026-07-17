package com.sanjay.ftgo.accounting.domain;

public record AuthorizeCardCommand(String eventId, Long orderId, Integer totalQuantity) {
}
