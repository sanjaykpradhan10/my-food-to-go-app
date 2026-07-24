package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderIdAllocationRepository extends JpaRepository<OrderIdAllocation, Long> {
}
