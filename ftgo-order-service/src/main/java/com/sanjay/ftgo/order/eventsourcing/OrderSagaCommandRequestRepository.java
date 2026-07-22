package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderSagaCommandRequestRepository extends JpaRepository<OrderSagaCommandRequest, Long> {

    List<OrderSagaCommandRequest> findByPublishedAtIsNullOrderByIdAsc();
}
