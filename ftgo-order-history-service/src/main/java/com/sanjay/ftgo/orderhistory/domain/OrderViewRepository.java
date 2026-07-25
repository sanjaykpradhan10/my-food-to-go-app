package com.sanjay.ftgo.orderhistory.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderViewRepository extends JpaRepository<OrderView, Long> {
}
