package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderEvent;
import com.sanjay.ftgo.order.domain.OrderEventSerializer;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

@Component
public class OrderEventStore {

    private static final int SNAPSHOT_EVERY_N_EVENTS = 5;

    private final OrderEventEntityRepository eventRepository;
    private final OrderIdAllocationRepository idAllocationRepository;
    private final OrderAggregateVersionRepository versionRepository;
    private final OrderSnapshotRepository snapshotRepository;
    private final OrderEventSerializer serializer;

    public OrderEventStore(OrderEventEntityRepository eventRepository,
                            OrderIdAllocationRepository idAllocationRepository,
                            OrderAggregateVersionRepository versionRepository,
                            OrderSnapshotRepository snapshotRepository,
                            OrderEventSerializer serializer) {
        this.eventRepository = eventRepository;
        this.idAllocationRepository = idAllocationRepository;
        this.versionRepository = versionRepository;
        this.snapshotRepository = snapshotRepository;
        this.serializer = serializer;
    }

    @Transactional
    public OrderAggregate save(CreateOrderCommand command, String triggeringEventId) {
        Long orderId = idAllocationRepository.save(new OrderIdAllocation()).getId();
        CreateOrderCommand withId =
                new CreateOrderCommand(orderId, command.consumerId(), command.restaurantId(), command.lineItems());

        OrderAggregate aggregate = new OrderAggregate();
        List<OrderDomainEvent> events = aggregate.process(withId);
        events.forEach(aggregate::apply);

        List<String> eventIds = appendEvents(orderId, events, triggeringEventId);
        versionRepository.save(new OrderAggregateVersion(orderId, lastEventId(eventIds, triggeringEventId)));
        maybeSnapshot(orderId, aggregate);
        return aggregate;
    }

    @Transactional(readOnly = true)
    public OrderAggregate find(Long orderId) {
        return replay(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    @Transactional
    public OrderAggregate update(Long orderId, Function<OrderAggregate, List<OrderDomainEvent>> process,
                                  String triggeringEventId) {
        // Load the version row via the repository and mutate it in place (never detach it) so
        // Hibernate's own dirty-checking flush performs the optimistic-lock @Version check, rather
        // than merge()'s reattachment semantics — see Task 4 review notes for why this matters.
        OrderAggregateVersion versionRow =
                versionRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
        OrderAggregate aggregate = replay(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));

        List<OrderDomainEvent> events = process.apply(aggregate);
        events.forEach(aggregate::apply);

        List<String> eventIds = appendEvents(orderId, events, triggeringEventId);
        versionRow.recordEvent(lastEventId(eventIds, triggeringEventId));
        versionRepository.save(versionRow);

        maybeSnapshot(orderId, aggregate);
        return aggregate;
    }

    @Transactional
    public void appendCompensationRequestedEvent(Long orderId, String triggeringEventId) {
        OrderAggregate aggregate = replay(orderId).orElse(null);
        if (aggregate == null || aggregate.getStatus() != com.sanjay.ftgo.order.domain.OrderStatus.REVISION_PENDING) {
            return;
        }
        String eventId = UUID.randomUUID().toString();
        OrderEvent wireEvent = new OrderEvent(eventId, "OrderRevisionCompensationRequested", orderId,
                null, null, toWireLineItems(aggregate.getLineItems()));
        eventRepository.save(new OrderEventEntity(
                eventId, wireEvent.eventType(), orderId, serializer.toJson(wireEvent), triggeringEventId));
    }

    private List<OrderEvent.LineItem> toWireLineItems(List<com.sanjay.ftgo.order.domain.OrderLineItem> lineItems) {
        return lineItems.stream()
                .map(lineItem -> new OrderEvent.LineItem(lineItem.menuItemId(), lineItem.quantity()))
                .toList();
    }

    private Optional<OrderAggregate> replay(Long orderId) {
        Optional<OrderSnapshot> snapshotOpt = snapshotRepository.findById(orderId);
        OrderAggregate aggregate;
        List<OrderEventEntity> tail;
        if (snapshotOpt.isPresent()) {
            OrderSnapshot snapshot = snapshotOpt.get();
            aggregate = OrderAggregate.fromSnapshot(readSnapshotData(snapshot.getSnapshotJson()));
            tail = eventRepository.findByOrderIdAndIdGreaterThanOrderByIdAsc(orderId, snapshot.getLastEventEntityId());
        } else {
            aggregate = new OrderAggregate();
            tail = eventRepository.findByOrderIdOrderByIdAsc(orderId);
        }
        if (snapshotOpt.isEmpty() && tail.isEmpty()) {
            return Optional.empty();
        }
        for (OrderEventEntity row : tail) {
            OrderEvent wireEvent = serializer.fromJson(row.getPayload());
            aggregate.apply(serializer.fromWireEvent(wireEvent));
        }
        return Optional.of(aggregate);
    }

    private List<String> appendEvents(Long orderId, List<OrderDomainEvent> events, String triggeringEventId) {
        List<String> eventIds = new ArrayList<>();
        for (OrderDomainEvent event : events) {
            String eventId = UUID.randomUUID().toString();
            OrderEvent wireEvent = serializer.toWireEvent(eventId, event);
            eventRepository.save(new OrderEventEntity(
                    eventId, wireEvent.eventType(), orderId, serializer.toJson(wireEvent), triggeringEventId));
            eventIds.add(eventId);
        }
        return eventIds;
    }

    private String lastEventId(List<String> eventIds, String fallback) {
        return eventIds.isEmpty() ? fallback : eventIds.get(eventIds.size() - 1);
    }

    private void maybeSnapshot(Long orderId, OrderAggregate aggregate) {
        long totalEvents = eventRepository.countByOrderId(orderId);
        if (totalEvents == 0 || totalEvents % SNAPSHOT_EVERY_N_EVENTS != 0) {
            return;
        }
        OrderEventEntity lastEvent = eventRepository.findTopByOrderIdOrderByIdDesc(orderId);
        String json = writeSnapshotData(aggregate.toSnapshotData());
        OrderSnapshot snapshot = snapshotRepository.findById(orderId)
                .map(existing -> existing.update(lastEvent.getId(), json))
                .orElseGet(() -> new OrderSnapshot(orderId, lastEvent.getId(), json));
        snapshotRepository.save(snapshot);
    }

    private OrderSnapshotData readSnapshotData(String json) {
        return serializer.readSnapshotData(json);
    }

    private String writeSnapshotData(OrderSnapshotData data) {
        return serializer.writeSnapshotData(data);
    }
}
