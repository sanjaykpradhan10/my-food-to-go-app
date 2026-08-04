package com.sanjay.ftgo.orderhistory.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderViewServiceTest {

    private final OrderViewRepository orderViewRepository = mock(OrderViewRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);
    private final MeterRegistry meterRegistry = mock(MeterRegistry.class);

    private OrderViewService orderViewService;

    @BeforeEach
    void setUp() {
        // Pre-existing tests don't exercise the counter, so stub it to a throwaway mock rather
        // than a real registry - keeps them focused on the upsert behavior they were written for.
        lenient().when(meterRegistry.counter(anyString())).thenReturn(mock(Counter.class));
        orderViewService = new OrderViewService(orderViewRepository, processedEventRepository, meterRegistry);
        when(orderViewRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    void orderCreatedCreatesFullRowWhenNoneExists() {
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.empty());
        List<OrderViewLineItem> lineItems = List.of(new OrderViewLineItem(10L, 2));

        orderViewService.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, lineItems);

        var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
        verify(orderViewRepository).save(captor.capture());
        OrderView saved = captor.getValue();
        assertThat(saved.getOrderId()).isEqualTo(42L);
        assertThat(saved.getConsumerId()).isEqualTo(1L);
        assertThat(saved.getRestaurantId()).isEqualTo(7L);
        assertThat(saved.getOrderStatus()).isEqualTo("APPROVAL_PENDING");
        assertThat(saved.getLineItems()).containsExactly(new OrderViewLineItem(10L, 2));
    }

    @Test
    void orderCreatedFillsInFieldsWhenRowAlreadyExists() {
        // Simulates the cross-topic race: a ticket/authorization/delivery event already
        // created a stub row before OrderCreated arrived.
        OrderView stub = new OrderView(42L);
        stub.setTicketStatus("CREATE_PENDING");
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(stub));
        List<OrderViewLineItem> lineItems = List.of(new OrderViewLineItem(10L, 2));

        orderViewService.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, lineItems);

        var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
        verify(orderViewRepository).save(captor.capture());
        OrderView saved = captor.getValue();
        assertThat(saved.getConsumerId()).isEqualTo(1L);
        assertThat(saved.getOrderStatus()).isEqualTo("APPROVAL_PENDING");
        assertThat(saved.getTicketStatus()).isEqualTo("CREATE_PENDING"); // untouched
    }

    @Test
    void orderApprovedUpdatesOrderStatusOnlyOnExistingRow() {
        OrderView existing = new OrderView(42L);
        existing.setOrderStatus("APPROVAL_PENDING");
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleOrderEvent("evt-2", "OrderApproved", 42L, null, null, null);

        assertThat(existing.getOrderStatus()).isEqualTo("APPROVED");
    }

    @Test
    void orderApprovedCreatesStubRowWhenNoneExists() {
        // Cross-topic race the other direction: OrderApproved somehow processed before
        // OrderCreated - shouldn't happen in practice (order-service publishes OrderCreated
        // first), but the upsert pattern must not NPE or drop the update either way.
        when(processedEventRepository.existsById("evt-2")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.empty());

        orderViewService.handleOrderEvent("evt-2", "OrderApproved", 42L, null, null, null);

        var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
        verify(orderViewRepository).save(captor.capture());
        assertThat(captor.getValue().getOrderStatus()).isEqualTo("APPROVED");
    }

    @Test
    void orderRevisionCompensationRequestedDoesNotChangeOrderStatus() {
        OrderView existing = new OrderView(42L);
        existing.setOrderStatus("REVISION_PENDING");
        when(processedEventRepository.existsById("evt-3")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleOrderEvent("evt-3", "OrderRevisionCompensationRequested", 42L, null, null, null);

        assertThat(existing.getOrderStatus()).isEqualTo("REVISION_PENDING");
    }

    @Test
    void orderRevisedUpdatesLineItems() {
        OrderView existing = new OrderView(42L);
        existing.setLineItems(List.of(new OrderViewLineItem(10L, 2)));
        existing.setOrderStatus("REVISION_PENDING");
        when(processedEventRepository.existsById("evt-16")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));
        List<OrderViewLineItem> revisedLineItems = List.of(new OrderViewLineItem(10L, 5), new OrderViewLineItem(20L, 1));

        orderViewService.handleOrderEvent("evt-16", "OrderRevised", 42L, null, null, revisedLineItems);

        assertThat(existing.getLineItems()).containsExactlyElementsOf(revisedLineItems);
        assertThat(existing.getOrderStatus()).isEqualTo("APPROVED");
    }

    // Full event-type-to-status mapping coverage for all 4 handlers, cross-checked against
    // README.md's "Events consumed" table (the authoritative list, independently re-verified
    // against the producing services' sealed-interface permits lists during Task 3-6 reviews).
    @ParameterizedTest
    @CsvSource({
            "OrderCreated,APPROVAL_PENDING",
            "OrderApproved,APPROVED",
            "OrderRejected,REJECTED",
            "OrderCancelled,CANCEL_PENDING",
            "OrderCancelConfirmed,CANCELLED",
            "OrderCancelRejected,APPROVED",
            "OrderRevisionProposed,REVISION_PENDING",
            "OrderRevised,APPROVED",
            "OrderRevisionRejected,APPROVED"
    })
    void orderEventTypeMapsToExpectedOrderStatus(String eventType, String expectedStatus) {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleOrderEvent("evt", eventType, 42L, 1L, 7L, List.of(new OrderViewLineItem(10L, 2)));

        assertThat(existing.getOrderStatus()).isEqualTo(expectedStatus);
    }

    @ParameterizedTest
    @CsvSource({
            "TicketCreated,CREATE_PENDING",
            "TicketConfirmed,AWAITING_ACCEPTANCE",
            "TicketAccepted,ACCEPTED",
            "TicketPreparingStarted,PREPARING",
            "TicketReadyForPickup,READY_FOR_PICKUP",
            "TicketPickedUp,PICKED_UP",
            "TicketCancelled,CANCELLED"
    })
    void kitchenEventTypeMapsToExpectedTicketStatus(String eventType, String expectedStatus) {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleKitchenEvent("evt", eventType, 42L);

        assertThat(existing.getTicketStatus()).isEqualTo(expectedStatus);
    }

    @ParameterizedTest
    @CsvSource({
            "CardAuthorized,AUTHORIZED",
            "CardAuthorizationFailed,DECLINED",
            "AuthorizationReversed,REVERSED",
            "AuthorizationRevised,AUTHORIZED"
    })
    void accountingEventTypeMapsToExpectedAuthorizationStatus(String eventType, String expectedStatus) {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleAccountingEvent("evt", eventType, 42L);

        assertThat(existing.getAuthorizationStatus()).isEqualTo(expectedStatus);
    }

    @ParameterizedTest
    @CsvSource({
            "DeliveryScheduled,SCHEDULED",
            "DeliveryPickedUp,PICKED_UP",
            "DeliveryDelivered,DELIVERED",
            "DeliveryCancelled,CANCELLED"
    })
    void deliveryEventTypeMapsToExpectedDeliveryStatus(String eventType, String expectedStatus) {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleDeliveryEvent("evt", eventType, 42L, 3L);

        assertThat(existing.getDeliveryStatus()).isEqualTo(expectedStatus);
    }

    @Test
    void dedupsOnEventId() {
        when(processedEventRepository.existsById("evt-1")).thenReturn(true);

        orderViewService.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, List.of());

        verify(orderViewRepository, org.mockito.Mockito.never()).findById(any());
    }

    @Test
    void handleOrderEventIncrementsOrderViewsUpdatedCounter() {
        SimpleMeterRegistry realMeterRegistry = new SimpleMeterRegistry();
        OrderViewService withMetrics = new OrderViewService(orderViewRepository, processedEventRepository, realMeterRegistry);
        when(processedEventRepository.existsById("evt-1")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.empty());

        withMetrics.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, List.of());

        assertThat(realMeterRegistry.counter("order_views_updated").count()).isEqualTo(1.0);
    }

    @Test
    void ticketCreatedSetsTicketStatusOnExistingOrCreatesStub() {
        when(processedEventRepository.existsById("evt-4")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.empty());

        orderViewService.handleKitchenEvent("evt-4", "TicketCreated", 42L);

        var captor = org.mockito.ArgumentCaptor.forClass(OrderView.class);
        verify(orderViewRepository).save(captor.capture());
        assertThat(captor.getValue().getTicketStatus()).isEqualTo("CREATE_PENDING");
    }

    @Test
    void ticketAcceptedUpdatesExistingRow() {
        OrderView existing = new OrderView(42L);
        existing.setTicketStatus("AWAITING_ACCEPTANCE");
        when(processedEventRepository.existsById("evt-5")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleKitchenEvent("evt-5", "TicketAccepted", 42L);

        assertThat(existing.getTicketStatus()).isEqualTo("ACCEPTED");
    }

    @Test
    void ticketCreationFailedDoesNotSetTicketStatus() {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt-6")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleKitchenEvent("evt-6", "TicketCreationFailed", 42L);

        assertThat(existing.getTicketStatus()).isNull();
    }

    @Test
    void ticketQuantityRevisedDoesNotChangeTicketStatus() {
        OrderView existing = new OrderView(42L);
        existing.setTicketStatus("ACCEPTED");
        when(processedEventRepository.existsById("evt-7")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleKitchenEvent("evt-7", "TicketQuantityRevised", 42L);

        assertThat(existing.getTicketStatus()).isEqualTo("ACCEPTED");
    }

    @Test
    void cardAuthorizedSetsAuthorizationStatus() {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt-8")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleAccountingEvent("evt-8", "CardAuthorized", 42L);

        assertThat(existing.getAuthorizationStatus()).isEqualTo("AUTHORIZED");
    }

    @Test
    void cardAuthorizationFailedSetsDeclined() {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt-9")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleAccountingEvent("evt-9", "CardAuthorizationFailed", 42L);

        assertThat(existing.getAuthorizationStatus()).isEqualTo("DECLINED");
    }

    @Test
    void authorizationReversedSetsReversed() {
        OrderView existing = new OrderView(42L);
        existing.setAuthorizationStatus("AUTHORIZED");
        when(processedEventRepository.existsById("evt-10")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleAccountingEvent("evt-10", "AuthorizationReversed", 42L);

        assertThat(existing.getAuthorizationStatus()).isEqualTo("REVERSED");
    }

    @Test
    void authorizationRevisionRejectedDoesNotChangeStatus() {
        OrderView existing = new OrderView(42L);
        existing.setAuthorizationStatus("AUTHORIZED");
        when(processedEventRepository.existsById("evt-11")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleAccountingEvent("evt-11", "AuthorizationRevisionRejected", 42L);

        assertThat(existing.getAuthorizationStatus()).isEqualTo("AUTHORIZED");
    }

    @Test
    void deliveryScheduledSetsStatusAndCourierId() {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt-12")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleDeliveryEvent("evt-12", "DeliveryScheduled", 42L, 3L);

        assertThat(existing.getDeliveryStatus()).isEqualTo("SCHEDULED");
        assertThat(existing.getCourierId()).isEqualTo(3L);
    }

    @Test
    void deliveryCancelledClearsCourierId() {
        OrderView existing = new OrderView(42L);
        existing.setDeliveryStatus("SCHEDULED");
        existing.setCourierId(3L);
        when(processedEventRepository.existsById("evt-13")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleDeliveryEvent("evt-13", "DeliveryCancelled", 42L, null);

        assertThat(existing.getDeliveryStatus()).isEqualTo("CANCELLED");
        assertThat(existing.getCourierId()).isNull();
    }

    @Test
    void deliverySchedulingFailedDoesNotChangeStatus() {
        OrderView existing = new OrderView(42L);
        when(processedEventRepository.existsById("evt-14")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleDeliveryEvent("evt-14", "DeliverySchedulingFailed", 42L, null);

        assertThat(existing.getDeliveryStatus()).isNull();
    }

    // Named for exactly what it tests - see also deliveryEventTypeMapsToExpectedDeliveryStatus
    // below, which covers DeliveryDelivered (and DeliveryPickedUp again) as part of the full
    // mapping table.
    @Test
    void deliveryPickedUpUpdatesStatus() {
        OrderView existing = new OrderView(42L);
        existing.setDeliveryStatus("SCHEDULED");
        when(processedEventRepository.existsById("evt-15")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleDeliveryEvent("evt-15", "DeliveryPickedUp", 42L, null);

        assertThat(existing.getDeliveryStatus()).isEqualTo("PICKED_UP");
    }
}
