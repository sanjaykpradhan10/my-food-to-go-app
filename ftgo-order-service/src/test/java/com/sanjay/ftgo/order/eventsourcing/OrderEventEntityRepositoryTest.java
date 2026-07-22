package com.sanjay.ftgo.order.eventsourcing;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.OptimisticLockingFailureException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
class OrderEventEntityRepositoryTest {

    @Autowired
    private OrderEventEntityRepository eventRepository;

    @Autowired
    private OrderIdAllocationRepository idAllocationRepository;

    @Autowired
    private OrderAggregateVersionRepository versionRepository;

    @Autowired
    private jakarta.persistence.EntityManager entityManager;

    @Test
    void allocatesIncreasingIds() {
        Long first = idAllocationRepository.save(new OrderIdAllocation()).getId();
        Long second = idAllocationRepository.save(new OrderIdAllocation()).getId();

        assertThat(second).isGreaterThan(first);
    }

    @Test
    void findsEventsInInsertOrder() {
        eventRepository.save(new OrderEventEntity("evt-1", "OrderCreated", 42L, "{}", "trigger-1"));
        eventRepository.save(new OrderEventEntity("evt-2", "OrderApproved", 42L, "{}", "trigger-2"));
        eventRepository.save(new OrderEventEntity("evt-3", "OrderApproved", 99L, "{}", "trigger-3"));

        List<OrderEventEntity> events = eventRepository.findByOrderIdOrderByIdAsc(42L);

        assertThat(events).extracting(OrderEventEntity::getEventId).containsExactly("evt-1", "evt-2");
    }

    @Test
    void concurrentVersionUpdateThrowsOptimisticLockingFailure() {
        versionRepository.saveAndFlush(new OrderAggregateVersion(42L, "evt-1"));
        entityManager.clear();

        // Load both copies while the row is still at version 0, then clear the persistence
        // context so BOTH become independently detached. Detaching only `first` leaves `second`
        // managed in the context, and Hibernate's merge(first) then silently reuses `second`'s
        // still-managed instance as its merge target instead of treating them as independent
        // stale reads - which defeats the whole point of this test.
        OrderAggregateVersion first = versionRepository.findById(42L).orElseThrow();
        OrderAggregateVersion second = versionRepository.findById(42L).orElseThrow();
        entityManager.clear();

        first.recordEvent("evt-2");
        versionRepository.saveAndFlush(first);
        entityManager.clear();

        second.recordEvent("evt-3");
        assertThatThrownBy(() -> versionRepository.saveAndFlush(second))
                .isInstanceOf(OptimisticLockingFailureException.class);
    }
}
