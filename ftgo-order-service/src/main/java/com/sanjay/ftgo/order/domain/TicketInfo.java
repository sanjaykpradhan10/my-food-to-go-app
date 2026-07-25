package com.sanjay.ftgo.order.domain;

import java.time.ZonedDateTime;

public record TicketInfo(Long id, Long orderId, String status, ZonedDateTime readyBy) {
}
