package com.sanjay.ftgo.delivery.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjay.ftgo.common.outbox.OutboxEventRepository;
import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

    private DeliveryService deliveryService;

    @BeforeEach
    void setUp() {
        deliveryService = new DeliveryService(deliveryRepository, courierRepository, processedEventRepository,
                failedOrderRepository, outboxEventRepository, domainEventPublisher, objectMapper);
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
        verify(domainEventPublisher).publish(any(Delivery.class), any());
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
        when(deliveryRepository.findByOrderId(42L)).thenReturn(Optional.of(delivery));
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
        when(deliveryRepository.findByOrderId(42L)).thenReturn(Optional.empty());

        deliveryService.release("evt-2", 42L);

        verify(failedOrderRepository).save(any(FailedOrder.class));
        verify(domainEventPublisher, never()).publish(any(), any());
    }

    @Test
    void handleScheduleDeliveryCommandRepliesDeliveryScheduled() {
        Courier courier = new Courier("Alex");
        when(courierRepository.findFirstByAvailableTrue()).thenReturn(Optional.of(courier));
        when(processedEventRepository.existsById("evt-3")).thenReturn(false);
        when(deliveryRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        deliveryService.handleScheduleDeliveryCommand("evt-3", 42L, 7L);

        verify(outboxEventRepository, times(1)).save(any());
    }

    @Test
    void handleReleaseDeliveryCommandRepliesDeliveryCancelled() {
        Delivery delivery = Delivery.schedule(42L, 7L, 9L).delivery();
        Courier courier = new Courier("Alex");
        courier.setAvailable(false);
        when(processedEventRepository.existsById("evt-4")).thenReturn(false);
        when(deliveryRepository.findByOrderId(42L)).thenReturn(Optional.of(delivery));
        when(courierRepository.findById(9L)).thenReturn(Optional.of(courier));

        deliveryService.handleReleaseDeliveryCommand("evt-4", 42L, "CreateOrder");

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        assertThat(courier.isAvailable()).isTrue();
        verify(outboxEventRepository, times(1)).save(any());
    }
}
