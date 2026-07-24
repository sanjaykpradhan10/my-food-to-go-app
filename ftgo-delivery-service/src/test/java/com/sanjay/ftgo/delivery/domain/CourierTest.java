package com.sanjay.ftgo.delivery.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CourierTest {

    @Test
    void newCourierStartsAvailable() {
        Courier courier = new Courier("Alex");

        assertThat(courier.getName()).isEqualTo("Alex");
        assertThat(courier.isAvailable()).isTrue();
    }

    @Test
    void setAvailableTogglesFlag() {
        Courier courier = new Courier("Alex");

        courier.setAvailable(false);

        assertThat(courier.isAvailable()).isFalse();
    }
}
