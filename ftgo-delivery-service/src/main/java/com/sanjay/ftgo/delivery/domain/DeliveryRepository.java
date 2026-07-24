package com.sanjay.ftgo.delivery.domain;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import java.util.Optional;

public interface DeliveryRepository extends JpaRepository<Delivery, Long> {

    Optional<Delivery> findByOrderId(Long orderId);

    // PESSIMISTIC_WRITE closes the race where two independent compensation triggers for the
    // same order (e.g. accounting's CardAuthorizationFailed and kitchen's resulting
    // TicketCancelled) are consumed concurrently by different Kafka listener threads: without
    // the row lock, both transactions can read status=SCHEDULED before either commits its
    // cancel, each thinking it's the one performing the cancellation. This serializes the two
    // release() calls so the second sees the already-CANCELLED row and no-ops (see
    // Delivery.cancel()).
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Delivery> findForUpdateByOrderId(Long orderId);
}
