package com.sanjay.ftgo.delivery.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryTest {

    @Test
    void scheduleStartsInScheduledAndEmitsDeliveryScheduled() {
        DeliveryScheduleResult result = Delivery.schedule(42L, 7L, 3L);

        assertThat(result.delivery().getStatus()).isEqualTo(DeliveryStatus.SCHEDULED);
        assertThat(result.delivery().getOrderId()).isEqualTo(42L);
        assertThat(result.delivery().getRestaurantId()).isEqualTo(7L);
        assertThat(result.delivery().getCourierId()).isEqualTo(3L);
        assertThat(result.events()).containsExactly(new DeliveryScheduledEvent(42L, 3L));
    }

    @Test
    void pickUpMovesFromScheduledToPickedUp() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        List<DeliveryDomainEvent> events = delivery.pickUp();

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.PICKED_UP);
        assertThat(events).containsExactly(new DeliveryPickedUpEvent(42L));
    }

    @Test
    void pickUpFromWrongStateThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();

        assertThatThrownBy(delivery::pickUp).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void deliverMovesFromPickedUpToDelivered() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();

        List<DeliveryDomainEvent> events = delivery.deliver();

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.DELIVERED);
        assertThat(events).containsExactly(new DeliveryDeliveredEvent(42L));
    }

    @Test
    void deliverFromWrongStateThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        assertThatThrownBy(delivery::deliver).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelMovesFromScheduledToCancelled() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();

        List<DeliveryDomainEvent> events = delivery.cancel();

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        assertThat(events).containsExactly(new DeliveryCancelledEvent(42L));
    }

    @Test
    void cancelFromPickedUpThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();

        assertThatThrownBy(delivery::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromDeliveredThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.pickUp();
        delivery.deliver();

        assertThatThrownBy(delivery::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void cancelFromCancelledThrows() {
        Delivery delivery = Delivery.schedule(42L, 7L, 3L).delivery();
        delivery.cancel();

        assertThatThrownBy(delivery::cancel).isInstanceOf(UnsupportedStateTransitionException.class);
    }
}
