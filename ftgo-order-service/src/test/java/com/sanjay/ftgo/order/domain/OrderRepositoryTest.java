package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
class OrderRepositoryTest {

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void revisesAndConfirmsAnOrderThroughActualPersistence() {
        Order order = new Order(1L, 1L, new ArrayList<>(List.of(new OrderLineItem(10L, 2))), OrderStatus.APPROVED);
        Order saved = orderRepository.save(order);

        Order toRevise = orderRepository.findById(saved.getId()).orElseThrow();
        List<OrderLineItem> revisedLineItems = new ArrayList<>(
                List.of(new OrderLineItem(10L, 5), new OrderLineItem(20L, 1)));
        toRevise.revise(new OrderRevision(revisedLineItems));
        orderRepository.save(toRevise);

        Order toConfirm = orderRepository.findById(saved.getId()).orElseThrow();

        // This is the call that would have thrown Hibernate's "Found shared references to a
        // collection" exception before confirmRevision() was fixed to copy pendingRevisedLineItems
        // into a fresh list instead of re-homing the same managed PersistentCollection instance.
        toConfirm.confirmRevision();
        orderRepository.save(toConfirm);

        Optional<Order> found = orderRepository.findById(saved.getId());

        assertThat(found).isPresent();
        assertThat(found.get().getLineItems()).containsExactlyInAnyOrderElementsOf(revisedLineItems);
        assertThat(found.get().getStatus()).isEqualTo(OrderStatus.APPROVED);
        assertThat(found.get().getPendingRevisedLineItems()).isNull();
    }

    @Test
    void revisesAnOrderWithAnImmutableRevisedLineItemsListLikeOrderControllerProduces() {
        Order order = new Order(1L, 1L, new ArrayList<>(List.of(new OrderLineItem(10L, 2))), OrderStatus.APPROVED);
        Order saved = orderRepository.save(order);

        Order toRevise = orderRepository.findById(saved.getId()).orElseThrow();

        // Exact shape OrderController.revise() produces via request.lineItems().stream()
        //         .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
        //         .toList() - an IMMUTABLE list. This reproduced Hibernate's
        // UnsupportedOperationException on save() before revise() was fixed to defensively
        // copy into a mutable list.
        List<OrderLineItem> revisedLineItems = List.of(new OrderLineItem(10L, 5), new OrderLineItem(20L, 1))
                .stream()
                .map(item -> new OrderLineItem(item.menuItemId(), item.quantity()))
                .toList();

        toRevise.revise(new OrderRevision(revisedLineItems));
        orderRepository.save(toRevise);

        Order found = orderRepository.findById(saved.getId()).orElseThrow();
        assertThat(found.getPendingRevisedLineItems()).containsExactlyInAnyOrderElementsOf(revisedLineItems);
    }
}
