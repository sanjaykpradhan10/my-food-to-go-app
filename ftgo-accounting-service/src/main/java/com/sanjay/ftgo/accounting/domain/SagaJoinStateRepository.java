package com.sanjay.ftgo.accounting.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface SagaJoinStateRepository extends JpaRepository<SagaJoinState, Long> {

    // consumer.events/kitchen.events/delivery.events listener threads race to create the
    // join-state row for a newly placed order; INSERT IGNORE makes the create step atomic and
    // idempotent so only one thread's insert actually lands, instead of all three racing
    // findById()-then-insert and duplicate-keying on saga_join_state.PRIMARY.
    @Modifying
    @Query(value = "INSERT IGNORE INTO saga_join_state (order_id, consumer_verified, ticket_created, delivery_scheduled, failed, resolved, version) VALUES (:orderId, false, false, false, false, false, 0)", nativeQuery = true)
    void insertIfAbsent(@Param("orderId") Long orderId);

    // Pessimistic write lock serializes the three saga-leg handlers per orderId after the row
    // is guaranteed to exist, so the second/third handler blocks until the first commits its
    // flag update instead of overwriting a stale in-memory copy.
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from SagaJoinState s where s.orderId = :orderId")
    Optional<SagaJoinState> findByIdForUpdate(@Param("orderId") Long orderId);
}
