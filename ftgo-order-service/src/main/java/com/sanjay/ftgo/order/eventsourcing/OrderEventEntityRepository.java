package com.sanjay.ftgo.order.eventsourcing;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderEventEntityRepository extends JpaRepository<OrderEventEntity, Long> {

    List<OrderEventEntity> findByOrderIdOrderByIdAsc(Long orderId);

    List<OrderEventEntity> findByOrderIdAndIdGreaterThanOrderByIdAsc(Long orderId, Long id);

    long countByOrderId(Long orderId);

    OrderEventEntity findTopByOrderIdOrderByIdDesc(Long orderId);
}
