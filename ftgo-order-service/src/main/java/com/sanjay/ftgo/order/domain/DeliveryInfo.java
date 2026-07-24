package com.sanjay.ftgo.order.domain;

public record DeliveryInfo(Long id, Long orderId, String status, Long courierId) {
}
