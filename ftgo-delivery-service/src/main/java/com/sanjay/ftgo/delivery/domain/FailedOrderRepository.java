package com.sanjay.ftgo.delivery.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FailedOrderRepository extends JpaRepository<FailedOrder, Long> {
}
