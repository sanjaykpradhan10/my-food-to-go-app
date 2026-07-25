package com.sanjay.ftgo.order.domain;

public interface DeliveryServicePort {

    SectionResult<DeliveryInfo> findDelivery(Long orderId);
}
