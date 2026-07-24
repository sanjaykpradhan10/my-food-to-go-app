package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CreateOrderSagaInstanceTest {

    @Test
    void constructorInitializesLegsUnset() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 3);

        assertThat(instance.getOrderId()).isEqualTo(42L);
        assertThat(instance.getTotalQuantity()).isEqualTo(3);
        assertThat(instance.isConsumerVerified()).isFalse();
        assertThat(instance.isTicketCreated()).isFalse();
        assertThat(instance.isDeliveryScheduled()).isFalse();
        assertThat(instance.isFailed()).isFalse();
    }

    @Test
    void marksDeliveryScheduled() {
        CreateOrderSagaInstance instance = new CreateOrderSagaInstance(42L, 3);

        instance.markDeliveryScheduled();

        assertThat(instance.isDeliveryScheduled()).isTrue();
    }
}
