package com.sanjay.ftgo.delivery.domain;

public class DeliveryNotFoundException extends RuntimeException {

    public DeliveryNotFoundException(Long deliveryId) {
        super("Delivery not found: " + deliveryId);
    }
}
