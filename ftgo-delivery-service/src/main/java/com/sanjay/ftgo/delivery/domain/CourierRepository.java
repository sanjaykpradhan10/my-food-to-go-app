package com.sanjay.ftgo.delivery.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface CourierRepository extends JpaRepository<Courier, Long> {

    Optional<Courier> findFirstByAvailableTrue();
}
