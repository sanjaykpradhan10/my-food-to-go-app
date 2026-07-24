package com.sanjay.ftgo.order.domain;

public interface SagaCommandPublisher {

    void publish(String topic, String eventId, String eventType, Long orderId, Object command);
}
