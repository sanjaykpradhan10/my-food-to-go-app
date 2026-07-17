package com.sanjay.ftgo.consumer.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findBySentAtIsNullOrderByIdAsc();
}
