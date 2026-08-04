package com.sanjay.ftgo.delivery.api;

import com.sanjay.ftgo.delivery.domain.Delivery;
import com.sanjay.ftgo.delivery.domain.DeliveryDomainEventPublisher;
import com.sanjay.ftgo.delivery.domain.DeliveryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeliveryControllerMetricsTest {

    @Mock
    private DeliveryRepository deliveryRepository;
    @Mock
    private DeliveryDomainEventPublisher domainEventPublisher;

    private SimpleMeterRegistry meterRegistry;
    private DeliveryController controller;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        controller = new DeliveryController(deliveryRepository, domainEventPublisher, meterRegistry);
    }

    @Test
    void pickedUpIncrementsDeliveriesPickedUpCounter() {
        Delivery delivery = Delivery.schedule(42L, 7L, 1L).delivery();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        controller.pickedUp(1L);

        assertThat(meterRegistry.counter("deliveries_picked_up").count()).isEqualTo(1.0);
    }

    @Test
    void deliveredIncrementsDeliveriesDeliveredCounter() {
        Delivery delivery = Delivery.schedule(42L, 7L, 1L).delivery();
        delivery.pickUp();
        when(deliveryRepository.findById(1L)).thenReturn(Optional.of(delivery));

        controller.delivered(1L);

        assertThat(meterRegistry.counter("deliveries_delivered").count()).isEqualTo(1.0);
    }
}
