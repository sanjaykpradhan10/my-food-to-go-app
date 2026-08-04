package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeliveryServiceTest {

    private final DeliveryRepository deliveryRepository = mock(DeliveryRepository.class);
    private final CourierRepository courierRepository = mock(CourierRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final FailedOrderRepository failedOrderRepository = mock(FailedOrderRepository.class);
    private final OutboxEventRepository outboxEventRepository = mock(OutboxEventRepository.class);
    private final DeliveryDomainEventPublisher domainEventPublisher = mock(DeliveryDomainEventPublisher.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MeterRegistry meterRegistry = mock(MeterRegistry.class);

    private DeliveryService deliveryService;

    @BeforeEach
    void setUp() {
        // Plain mock() fixtures (no MockitoExtension) - stub counter() to hand back a no-op
        // Counter mock so schedule()/release()'s meterRegistry.counter(...).increment() calls
        // don't NPE on the tests below that don't care about metrics.
        when(meterRegistry.counter(anyString())).thenReturn(mock(Counter.class));
        deliveryService = new DeliveryService(deliveryRepository, courierRepository, processedEventRepository,
                failedOrderRepository, outboxEventRepository, domainEventPublisher, objectMapper, meterRegistry);
    }

    @Test
    void handleOrderCreatedSchedulesWhenCourierAvailable() {
        Courier courier = new Courier("Alex");
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.of(courier));
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(deliveryRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        deliveryService.handleOrderCreated("evt-1", 42L, 7L);

        assertThat(courier.isAvailable()).isFalse();
        verify(courierRepository).save(courier);
        verify(deliveryRepository).save(any(Delivery.class));
        verify(domainEventPublisher).publish(any(Delivery.class),
                eq(java.util.List.of(new DeliveryScheduledEvent(42L, courier.getId()))));
    }

    @Test
    void handleOrderCreatedPublishesSchedulingFailedWhenNoCourierAvailable() {
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.empty());
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);

        deliveryService.handleOrderCreated("evt-1", 42L, 7L);

        verify(deliveryRepository, never()).save(any());
        verify(domainEventPublisher).publishSchedulingFailed(new DeliverySchedulingFailedEvent(42L, "no courier available"));
    }

    @Test
    void handleOrderCreatedSkipsSchedulingWhenOrderAlreadyFailed() {
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(true);

        deliveryService.handleOrderCreated("evt-1", 42L, 7L);

        verify(courierRepository, never()).findFirstByAvailableTrue();
        verify(deliveryRepository, never()).save(any());
    }

    @Test
    void releaseCancelsDeliveryAndFreesCourier() {
        Courier courier = new Courier("Alex");
        courier.setAvailable(false);
        Delivery delivery = Delivery.schedule(42L, 7L, 9L).delivery();
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(deliveryRepository.findForUpdateByOrderId(42L)).thenReturn(Optional.of(delivery));
        when(courierRepository.findById(9L)).thenReturn(Optional.of(courier));

        deliveryService.release("evt-2", 42L);

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        assertThat(courier.isAvailable()).isTrue();
        verify(courierRepository).save(courier);
        verify(domainEventPublisher).publish(delivery, java.util.List.of(new DeliveryCancelledEvent(42L)));
    }

    @Test
    void releaseRecordsFailedOrderWhenDeliveryNotYetScheduled() {
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(deliveryRepository.findForUpdateByOrderId(42L)).thenReturn(Optional.empty());

        deliveryService.release("evt-2", 42L);

        verify(failedOrderRepository).save(any(FailedOrder.class));
        verify(domainEventPublisher, never()).publish(any(), any());
    }

    @Test
    void releaseIsIdempotentAgainstAlreadyCancelledDelivery() {
        // Simulates a second racing release() call that acquires the lock after a first racer
        // already committed the CANCELLED status - cancel() returns List.of() on this delivery.
        Delivery delivery = Delivery.schedule(42L, 7L, 9L).delivery();
        delivery.cancel();
        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        when(processedEventRepository.existsById("evt-2b")).thenReturn(false);
        when(deliveryRepository.findForUpdateByOrderId(42L)).thenReturn(Optional.of(delivery));

        deliveryService.release("evt-2b", 42L);

        // Must not re-touch the courier (it may have since been reassigned to a different order)
        // or publish a duplicate DeliveryCancelled domain event.
        verify(courierRepository, never()).findById(any());
        verify(courierRepository, never()).save(any());
        verify(domainEventPublisher, never()).publish(any(), any());
    }

    @Test
    void handleScheduleDeliveryCommandRepliesDeliveryScheduled() {
        Courier courier = new Courier("Alex");
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.of(courier));
        when(processedEventRepository.existsById("evt-3")).thenReturn(false);
        when(deliveryRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        deliveryService.handleScheduleDeliveryCommand("evt-3", 42L, 7L);

        org.mockito.ArgumentCaptor<com.sanjay.ftgo.common.outbox.OutboxEvent> captor =
                org.mockito.ArgumentCaptor.forClass(com.sanjay.ftgo.common.outbox.OutboxEvent.class);
        verify(outboxEventRepository, times(1)).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("DeliveryScheduled");
        assertThat(captor.getValue().getAggregateId()).isEqualTo(42L);
        assertThat(captor.getValue().getTopic()).isEqualTo("saga.replies");
    }

    @Test
    void handleReleaseDeliveryCommandRepliesDeliveryCancelled() {
        Delivery delivery = Delivery.schedule(42L, 7L, 9L).delivery();
        Courier courier = new Courier("Alex");
        courier.setAvailable(false);
        when(processedEventRepository.existsById("evt-4")).thenReturn(false);
        when(deliveryRepository.findForUpdateByOrderId(42L)).thenReturn(Optional.of(delivery));
        when(courierRepository.findById(9L)).thenReturn(Optional.of(courier));

        deliveryService.handleReleaseDeliveryCommand("evt-4", 42L, "CreateOrder");

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        assertThat(courier.isAvailable()).isTrue();
        org.mockito.ArgumentCaptor<com.sanjay.ftgo.common.outbox.OutboxEvent> captor =
                org.mockito.ArgumentCaptor.forClass(com.sanjay.ftgo.common.outbox.OutboxEvent.class);
        verify(outboxEventRepository, times(1)).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("DeliveryCancelled");
        assertThat(captor.getValue().getAggregateId()).isEqualTo(42L);
        assertThat(captor.getValue().getTopic()).isEqualTo("saga.replies");
    }

    @Test
    void handleReleaseDeliveryCommandIsIdempotentAgainstAlreadyCancelledDelivery() {
        // Simulates a second racing call to the orchestration entry point after a first racer
        // already committed the CANCELLED status - cancel() returns List.of() on this delivery.
        Delivery delivery = Delivery.schedule(42L, 7L, 9L).delivery();
        delivery.cancel();
        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        when(processedEventRepository.existsById("evt-4b")).thenReturn(false);
        when(deliveryRepository.findForUpdateByOrderId(42L)).thenReturn(Optional.of(delivery));

        deliveryService.handleReleaseDeliveryCommand("evt-4b", 42L, "CreateOrder");

        // Must not re-touch the courier or emit a duplicate DeliveryCancelled reply on saga.replies.
        // outboxEventRepository is only ever written to by publishReply in this service, so
        // never().save(any()) here is a direct assertion that no reply was published - not a
        // false negative against some other dedup-ledger usage of this repository.
        verify(courierRepository, never()).findById(any());
        verify(courierRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void handleOrderCreatedIncrementsDeliveriesScheduledCounter() {
        SimpleMeterRegistry realMeterRegistry = new SimpleMeterRegistry();
        DeliveryService withMetrics = new DeliveryService(deliveryRepository, courierRepository,
                processedEventRepository, failedOrderRepository, outboxEventRepository,
                domainEventPublisher, objectMapper, realMeterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(failedOrderRepository.existsById(42L)).thenReturn(false);
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.of(new Courier("Alex")));

        withMetrics.handleOrderCreated("evt-1", 42L, 7L);

        assertThat(realMeterRegistry.counter("deliveries_scheduled").count()).isEqualTo(1.0);
    }

    @Test
    void releaseIncrementsDeliveriesCancelledCounter() {
        SimpleMeterRegistry realMeterRegistry = new SimpleMeterRegistry();
        DeliveryService withMetrics = new DeliveryService(deliveryRepository, courierRepository,
                processedEventRepository, failedOrderRepository, outboxEventRepository,
                domainEventPublisher, objectMapper, realMeterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        Delivery delivery = Delivery.schedule(42L, 7L, 1L).delivery();
        when(deliveryRepository.findForUpdateByOrderId(42L)).thenReturn(Optional.of(delivery));

        withMetrics.release("evt-1", 42L);

        assertThat(realMeterRegistry.counter("deliveries_cancelled").count()).isEqualTo(1.0);
    }
}
