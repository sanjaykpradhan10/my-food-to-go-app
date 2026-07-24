package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderCreatedEvent;
import com.sanjay.ftgo.order.domain.OrderDomainEvent;
import com.sanjay.ftgo.order.domain.OrderEventSerializer;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderNotFoundException;
import com.sanjay.ftgo.order.domain.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// @DataJpaTest slices out JacksonAutoConfiguration, but OrderEventSerializer needs an ObjectMapper
// bean to (de)serialize event/snapshot payloads, so it's imported explicitly alongside the classes
// under test.
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({JacksonAutoConfiguration.class, OrderEventSerializer.class, OrderEventStore.class})
class OrderEventStoreTest {

    @Autowired
    private OrderEventStore eventStore;

    @Autowired
    private OrderEventEntityRepository eventRepository;

    @Autowired
    private OrderSnapshotRepository snapshotRepository;

    private OrderAggregate createOrder() {
        CreateOrderCommand command = new CreateOrderCommand(null, 1L, 1L, List.of(new OrderLineItem(10L, 2)));
        return eventStore.save(command, "trigger-create");
    }

    @Test
    void saveAllocatesIdAndPersistsCreatedEvent() {
        OrderAggregate aggregate = createOrder();

        assertThat(aggregate.getId()).isNotNull();
        assertThat(aggregate.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
        assertThat(eventRepository.findByOrderIdOrderByIdAsc(aggregate.getId())).hasSize(1);
        assertThat(eventRepository.findByOrderIdOrderByIdAsc(aggregate.getId()).get(0).getEventType())
                .isEqualTo("OrderCreated");
    }

    @Test
    void findReplaysPersistedEvents() {
        OrderAggregate created = createOrder();

        OrderAggregate found = eventStore.find(created.getId());

        assertThat(found.getStatus()).isEqualTo(OrderStatus.APPROVAL_PENDING);
        assertThat(found.getConsumerId()).isEqualTo(1L);
    }

    @Test
    void findThrowsWhenOrderDoesNotExist() {
        assertThatThrownBy(() -> eventStore.find(999L)).isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void updateAppliesCommandAndPersistsNewEvent() {
        OrderAggregate created = createOrder();

        OrderAggregate updated = eventStore.update(created.getId(),
                aggregate -> aggregate.process(new ApproveOrderCommand()), "trigger-approve");

        assertThat(updated.getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(eventRepository.findByOrderIdOrderByIdAsc(created.getId())).hasSize(2);

        OrderAggregate reloaded = eventStore.find(created.getId());
        assertThat(reloaded.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void updateThrowsWhenOrderDoesNotExist() {
        assertThatThrownBy(() -> eventStore.update(999L,
                aggregate -> aggregate.process(new ApproveOrderCommand()), "trigger"))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void replaySkipsNonReplayableCompensationRequestedEvent() {
        OrderAggregate created = createOrder();
        Long orderId = created.getId();
        eventStore.update(orderId, aggregate -> aggregate.process(new ApproveOrderCommand()), "t2");
        eventStore.update(orderId, aggregate -> aggregate.process(new ReviseOrderCommand(
                new com.sanjay.ftgo.order.domain.OrderRevision(List.of(new OrderLineItem(10L, 5))))), "t3");

        eventStore.appendCompensationRequestedEvent(orderId, "t4");

        OrderAggregate reloaded = eventStore.find(orderId);
        assertThat(reloaded.getStatus()).isEqualTo(OrderStatus.REVISION_PENDING);

        OrderAggregate afterUpdate = eventStore.update(orderId,
                aggregate -> aggregate.process(new RejectRevisionCommand()), "t5");
        assertThat(afterUpdate.getStatus()).isEqualTo(OrderStatus.APPROVED);
    }

    @Test
    void writesSnapshotAfterConfiguredEventThreshold() {
        OrderAggregate created = createOrder();
        Long orderId = created.getId();

        // 1 create + 4 further event-producing transitions = 5 events, tripping the threshold.
        // (A trailing no-op update was tried first but never gets total events to 5, since it
        // contributes zero events - replaced with a real 5th transition: cancel -> undo -> cancel.)
        eventStore.update(orderId, aggregate -> aggregate.process(new ApproveOrderCommand()), "t2");
        eventStore.update(orderId, aggregate -> aggregate.process(new CancelOrderCommand()), "t3");
        eventStore.update(orderId, aggregate -> aggregate.process(new UndoCancelCommand()), "t4");
        eventStore.update(orderId, aggregate -> aggregate.process(new CancelOrderCommand()), "t5");

        assertThat(snapshotRepository.findById(orderId)).isPresent();
    }
}
