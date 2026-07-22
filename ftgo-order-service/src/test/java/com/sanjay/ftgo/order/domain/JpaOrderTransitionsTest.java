package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JpaOrderTransitionsTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderDomainEventPublisher domainEventPublisher;

    @InjectMocks
    private JpaOrderTransitions transitions;

    private Order orderIn(OrderStatus status) {
        return new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), status);
    }

    @Test
    void createSavesNewOrder() {
        when(orderRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Order created = transitions.create(1L, 1L, List.of(new OrderLineItem(10L, 2)), "evt-1");

        assertThat(created.getConsumerId()).isEqualTo(1L);
        assertThat(created.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
    }

    @Test
    void cancelThrowsWhenOrderNotFound() {
        when(orderRepository.findById(42L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> transitions.cancel(42L, "evt-1")).isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void cancelThrowsWhenWrongState() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVAL_PENDING)));

        assertThatThrownBy(() -> transitions.cancel(42L, "evt-1")).isInstanceOf(OrderCannotBeCancelledException.class);
    }

    @Test
    void cancelSavesAndReturnsEvents() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVED)));

        TransitionResult result = transitions.cancel(42L, "evt-1");

        assertThat(result.order().getStatus()).isEqualTo(OrderStatus.CANCEL_PENDING);
        assertThat(result.events()).containsExactly(new OrderCancelledEvent(42L));
        verify(orderRepository).save(result.order());
    }

    @Test
    void approveSilentlyNoOpsWhenOrderNotFound() {
        when(orderRepository.findById(42L)).thenReturn(Optional.empty());

        transitions.approve(42L, "evt-1");

        verify(orderRepository, never()).save(any());
        verify(domainEventPublisher, never()).publish(any());
    }

    @Test
    void approveSilentlyNoOpsWhenWrongState() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVED)));

        transitions.approve(42L, "evt-1");

        verify(orderRepository, never()).save(any());
        verify(domainEventPublisher, never()).publish(any());
    }

    @Test
    void approveSavesAndPublishesOnSuccess() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVAL_PENDING)));

        transitions.approve(42L, "evt-1");

        verify(orderRepository).save(any());
        verify(domainEventPublisher).publish(List.of(new OrderApprovedEvent(42L)));
    }

    // reject() shares applyBestEffort's orchestration with approve() (only the Order method
    // reference differs), but that sharing is exactly why a dedicated test matters here - it's
    // the only place that would catch the method reference itself being wrong (e.g. accidentally
    // wired to Order::noteApproved instead of Order::noteRejected).
    @Test
    void rejectSavesAndPublishesOnSuccess() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVAL_PENDING)));

        transitions.reject(42L, "evt-1");

        verify(orderRepository).save(any());
        verify(domainEventPublisher).publish(List.of(new OrderRejectedEvent(42L)));
    }

    // noteCancelled()/undoCancel() share applyBestEffort's orchestration with approve()/reject()
    // (only the Order method reference differs) — dedicated tests are the only place that would
    // catch the method reference itself being wrong.
    @Test
    void noteCancelledSavesAndPublishesOnSuccess() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.CANCEL_PENDING)));

        transitions.noteCancelled(42L, "evt-1");

        verify(orderRepository).save(any());
        verify(domainEventPublisher).publish(List.of(new OrderCancelConfirmedEvent(42L)));
    }

    @Test
    void undoCancelSavesAndPublishesOnSuccess() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.CANCEL_PENDING)));

        transitions.undoCancel(42L, "evt-1");

        verify(orderRepository).save(any());
        verify(domainEventPublisher).publish(List.of(new OrderCancelRejectedEvent(42L)));
    }

    // confirmRevision()/rejectRevision() share applyBestEffort's orchestration with the other
    // transitions (only the Order method reference differs) — dedicated tests are the only place
    // that would catch the method reference itself being wrong.
    @Test
    void confirmRevisionSavesAndPublishesOnSuccess() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.REVISION_PENDING,
                List.of(new OrderLineItem(10L, 8)));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        transitions.confirmRevision(42L, "evt-1");

        verify(orderRepository).save(any());
        verify(domainEventPublisher).publish(List.of(new OrderRevisedEvent(42L, List.of(new OrderLineItem(10L, 8)))));
    }

    @Test
    void rejectRevisionSavesAndPublishesOnSuccess() {
        Order order = new Order(42L, 1L, 1L, List.of(new OrderLineItem(10L, 2)), OrderStatus.REVISION_PENDING,
                List.of(new OrderLineItem(10L, 8)));
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        transitions.rejectRevision(42L, "evt-1");

        verify(orderRepository).save(any());
        verify(domainEventPublisher).publish(List.of(new OrderRevisionRejectedEvent(42L)));
    }

    @Test
    void requestRevisionCompensationSilentlyNoOpsWhenOrderNotFound() {
        when(orderRepository.findById(42L)).thenReturn(Optional.empty());

        transitions.requestRevisionCompensation(42L, "evt-1");

        verify(domainEventPublisher, never()).publishRevisionCompensationRequested(any(), any());
    }

    @Test
    void requestRevisionCompensationSilentlyNoOpsWhenNotRevisionPending() {
        when(orderRepository.findById(42L)).thenReturn(Optional.of(orderIn(OrderStatus.APPROVED)));

        transitions.requestRevisionCompensation(42L, "evt-1");

        verify(domainEventPublisher, never()).publishRevisionCompensationRequested(any(), any());
    }

    @Test
    void requestRevisionCompensationPublishesWithAFreshEventIdWhenRevisionPending() {
        Order order = orderIn(OrderStatus.REVISION_PENDING);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        transitions.requestRevisionCompensation(42L, "evt-1");

        org.mockito.ArgumentCaptor<String> eventIdCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(domainEventPublisher).publishRevisionCompensationRequested(eq(order), eventIdCaptor.capture());
        assertThat(eventIdCaptor.getValue()).isNotEqualTo("evt-1");
    }
}
