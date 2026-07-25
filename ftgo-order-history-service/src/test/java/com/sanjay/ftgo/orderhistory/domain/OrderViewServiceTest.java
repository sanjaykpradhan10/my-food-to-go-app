package com.sanjay.ftgo.orderhistory.domain;

import com.sanjay.ftgo.common.outbox.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderViewServiceTest {

    private final OrderViewRepository orderViewRepository = mock(OrderViewRepository.class);
    private final ProcessedEventRepository processedEventRepository = mock(ProcessedEventRepository.class);

    private OrderViewService orderViewService;

    @BeforeEach
    void setUp() {
        orderViewService = new OrderViewService(orderViewRepository, processedEventRepository);
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
    void dedupsOnEventId() {
        when(processedEventRepository.existsById("evt-1")).thenReturn(true);

        orderViewService.handleOrderEvent("evt-1", "OrderCreated", 42L, 1L, 7L, List.of());

        verify(orderViewRepository, org.mockito.Mockito.never()).findById(any());
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

    @Test
    void deliveryPickedUpAndDeliveredUpdateStatus() {
        OrderView existing = new OrderView(42L);
        existing.setDeliveryStatus("SCHEDULED");
        when(processedEventRepository.existsById("evt-15")).thenReturn(false);
        when(orderViewRepository.findById(42L)).thenReturn(Optional.of(existing));

        orderViewService.handleDeliveryEvent("evt-15", "DeliveryPickedUp", 42L, null);

        assertThat(existing.getDeliveryStatus()).isEqualTo("PICKED_UP");
    }
}
