package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderRevision;
import com.sanjay.ftgo.order.domain.OrderTransitions;
import com.sanjay.ftgo.order.domain.TransitionResult;
import com.sanjay.ftgo.order.domain.UnsupportedStateTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "persistence.mode", havingValue = "event-sourcing")
public class EventSourcedOrderTransitions implements OrderTransitions {

    private static final Logger log = LoggerFactory.getLogger(EventSourcedOrderTransitions.class);

    private final OrderEventStore eventStore;

    public EventSourcedOrderTransitions(OrderEventStore eventStore) {
        this.eventStore = eventStore;
    }

    @Override
    public Order create(Long consumerId, Long restaurantId, List<OrderLineItem> lineItems, String eventId) {
        CreateOrderCommand command = new CreateOrderCommand(null, consumerId, restaurantId, lineItems);
        return toOrder(eventStore.save(command, eventId));
    }

    @Override
    public java.util.Optional<Order> findById(Long orderId) {
        try {
            return java.util.Optional.of(toOrder(eventStore.find(orderId)));
        } catch (com.sanjay.ftgo.order.domain.OrderNotFoundException e) {
            return java.util.Optional.empty();
        }
    }

    @Override
    public TransitionResult cancel(Long orderId, String eventId) {
        OrderAggregate aggregate =
                eventStore.update(orderId, a -> a.process(new CancelOrderCommand()), eventId);
        return new TransitionResult(toOrder(aggregate), List.of(new com.sanjay.ftgo.order.domain.OrderCancelledEvent(orderId)));
    }

    @Override
    public TransitionResult revise(Long orderId, OrderRevision revision, String eventId) {
        OrderAggregate aggregate =
                eventStore.update(orderId, a -> a.process(new ReviseOrderCommand(revision)), eventId);
        return new TransitionResult(toOrder(aggregate),
                List.of(new com.sanjay.ftgo.order.domain.OrderRevisionProposedEvent(orderId, revision.revisedLineItems())));
    }

    @Override
    public void approve(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new ApproveOrderCommand()), eventId, "approve");
    }

    @Override
    public void reject(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new RejectOrderCommand()), eventId, "reject");
    }

    @Override
    public void noteCancelled(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new NoteOrderCancelledCommand()), eventId, "cancel confirmation");
    }

    @Override
    public void undoCancel(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new UndoCancelCommand()), eventId, "cancel rejection");
    }

    @Override
    public void confirmRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new ConfirmRevisionCommand()), eventId, "revision confirmation");
    }

    @Override
    public void rejectRevision(Long orderId, String eventId) {
        applyBestEffort(orderId, a -> a.process(new RejectRevisionCommand()), eventId, "revision rejection");
    }

    @Override
    public void requestRevisionCompensation(Long orderId, String eventId) {
        // Order itself doesn't transition here, mirroring JpaOrderTransitions' equivalent — kitchen's
        // undo is what needs to run before Order leaves REVISION_PENDING. Recorded as an
        // OrderAggregate-bypassing wire event, same deliberate exception the JPA path takes via
        // OrderDomainEventPublisher.publishRevisionCompensationRequested.
        eventStore.appendCompensationRequestedEvent(orderId, eventId);
    }

    private void applyBestEffort(Long orderId,
                                  java.util.function.Function<OrderAggregate, List<com.sanjay.ftgo.order.domain.OrderDomainEvent>> process,
                                  String eventId, String description) {
        try {
            eventStore.update(orderId, process, eventId);
        } catch (com.sanjay.ftgo.order.domain.OrderNotFoundException e) {
            log.debug("Ignoring {} for unknown order {}", description, orderId);
        } catch (UnsupportedStateTransitionException | com.sanjay.ftgo.order.domain.OrderCannotBeCancelledException e) {
            log.debug("Ignoring {} for order {}: {}", description, orderId, e.getMessage());
        }
    }

    private Order toOrder(OrderAggregate aggregate) {
        return new Order(aggregate.getId(), aggregate.getConsumerId(), aggregate.getRestaurantId(),
                aggregate.getLineItems(), aggregate.getStatus(), aggregate.getPendingRevisedLineItems());
    }
}
