package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderAggregateVersionRepository extends JpaRepository<OrderAggregateVersion, Long> {
}
