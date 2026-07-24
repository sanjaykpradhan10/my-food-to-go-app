package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderEventSerializerTest {

    private final OrderEventSerializer serializer = new OrderEventSerializer(new com.fasterxml.jackson.databind.ObjectMapper());

    @Test
    void roundTripsOrderCreatedEvent() {
        OrderCreatedEvent event = new OrderCreatedEvent(42L, 1L, 2L, List.of(new OrderLineItem(10L, 3)));

        OrderEvent wire = serializer.toWireEvent("evt-1", event);
        assertThat(wire.eventType()).isEqualTo("OrderCreated");
        assertThat(wire.orderId()).isEqualTo(42L);
        assertThat(wire.consumerId()).isEqualTo(1L);
        assertThat(wire.restaurantId()).isEqualTo(2L);

        OrderDomainEvent roundTripped = serializer.fromWireEvent(wire);
        assertThat(roundTripped).isEqualTo(event);
    }

    @Test
    void roundTripsOrderApprovedEvent() {
        OrderApprovedEvent event = new OrderApprovedEvent(42L);

        OrderEvent wire = serializer.toWireEvent("evt-2", event);
        OrderDomainEvent roundTripped = serializer.fromWireEvent(wire);

        assertThat(roundTripped).isEqualTo(event);
    }

    @Test
    void roundTripsOrderRevisionProposedEvent() {
        OrderRevisionProposedEvent event = new OrderRevisionProposedEvent(42L, List.of(new OrderLineItem(10L, 5)));

        OrderEvent wire = serializer.toWireEvent("evt-3", event);
        OrderDomainEvent roundTripped = serializer.fromWireEvent(wire);

        assertThat(roundTripped).isEqualTo(event);
    }

    @Test
    void jsonRoundTrip() {
        OrderEvent wire = new OrderEvent("evt-4", "OrderApproved", 42L, null, null, null);

        String json = serializer.toJson(wire);
        OrderEvent parsed = serializer.fromJson(json);

        assertThat(parsed).isEqualTo(wire);
    }
}
