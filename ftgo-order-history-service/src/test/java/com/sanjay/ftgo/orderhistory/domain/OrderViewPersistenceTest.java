package com.sanjay.ftgo.orderhistory.domain;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Real Hibernate + H2 (via @DataJpaTest), not mocks - both findings below are about how
// Hibernate actually behaves on save()/merge() for an entity with an externally-assigned @Id,
// which a mocked repository can't reproduce.
@DataJpaTest
class OrderViewPersistenceTest {

    @Autowired
    private OrderViewRepository orderViewRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    // REQUIRES_NEW so each call below runs (and commits) in its own transaction/persistence
    // context, standing in for the independent @KafkaListener consumer threads that each open
    // their own @Transactional handler invocation against the same row.
    private TransactionTemplate newTransactionTemplate() {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        return template;
    }

    @Test
    void secondConcurrentSaveOfSameRowThrowsOptimisticLockingFailureExceptionInsteadOfOverwriting() {
        TransactionTemplate tx = newTransactionTemplate();
        tx.executeWithoutResult(status -> orderViewRepository.save(new OrderView(42L)));

        // Two independent "threads" each load their own managed copy of the same row before
        // either has written back - both see version 0, exactly like OrderCreated's and
        // TicketCreated's listener threads racing on the same orderId.
        OrderView loadedByThreadA = tx.execute(status -> orderViewRepository.findById(42L).orElseThrow());
        OrderView loadedByThreadB = tx.execute(status -> orderViewRepository.findById(42L).orElseThrow());

        // Thread A (e.g. OrderCreated's handler) wins the race and commits first.
        tx.executeWithoutResult(status -> {
            loadedByThreadA.setConsumerId(1L);
            orderViewRepository.save(loadedByThreadA);
        });

        // Thread B (e.g. TicketCreated's handler) still holds its stale version-0 snapshot.
        // Without @Version this merge() would silently overwrite consumerId back to null;
        // with @Version it must instead fail loudly so KafkaConsumerConfig's error handler
        // retries the whole listener invocation against a fresh read.
        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            loadedByThreadB.setTicketStatus("CREATE_PENDING");
            orderViewRepository.save(loadedByThreadB);
        })).isInstanceOf(OptimisticLockingFailureException.class);

        OrderView finalState = tx.execute(status -> orderViewRepository.findById(42L).orElseThrow());
        // Thread A's write survived; thread B's was rejected outright rather than silently
        // discarding thread A's consumerId, proving there's no lost update.
        assertThat(finalState.getConsumerId()).isEqualTo(1L);
        assertThat(finalState.getTicketStatus()).isNull();
    }

    @Test
    void setLineItemsWithAnImmutableListLikeOrderEventListenerProducesSurvivesSave() {
        // Exact shape OrderEventListener.onMessage() produces via
        // event.lineItems().stream().map(...).toList() - an IMMUTABLE list. Without the
        // defensive copy in OrderView.setLineItems(), Hibernate throws
        // UnsupportedOperationException when it clears the @ElementCollection during merge().
        List<OrderViewLineItem> immutableLineItems = List.of(new OrderViewLineItem(10L, 2)).stream()
                .map(li -> new OrderViewLineItem(li.menuItemId(), li.quantity()))
                .toList();

        TransactionTemplate tx = newTransactionTemplate();
        tx.executeWithoutResult(status -> {
            OrderView view = new OrderView(99L);
            view.setLineItems(immutableLineItems);
            orderViewRepository.save(view);
        });

        // Re-saving with a second immutable list exercises the merge()-clears-the-collection
        // path again, on an already-persisted row.
        List<OrderViewLineItem> replacementImmutableLineItems = List.of(new OrderViewLineItem(20L, 1)).stream()
                .map(li -> new OrderViewLineItem(li.menuItemId(), li.quantity()))
                .toList();
        tx.executeWithoutResult(status -> {
            OrderView view = orderViewRepository.findById(99L).orElseThrow();
            view.setLineItems(replacementImmutableLineItems);
            orderViewRepository.save(view);
        });

        OrderView found = orderViewRepository.findById(99L).orElseThrow();
        assertThat(found.getLineItems()).containsExactly(new OrderViewLineItem(20L, 1));
    }
}
