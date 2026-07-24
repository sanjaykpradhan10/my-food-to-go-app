package com.sanjay.ftgo.delivery.domain;

import java.util.List;

public record DeliveryScheduleResult(Delivery delivery, List<DeliveryDomainEvent> events) {
}
