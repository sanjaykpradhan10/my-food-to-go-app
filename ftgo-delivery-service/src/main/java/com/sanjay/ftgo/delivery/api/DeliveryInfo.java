package com.sanjay.ftgo.delivery.api;

public record DeliveryInfo(Long id, Long orderId, String status, Long courierId) {
}
