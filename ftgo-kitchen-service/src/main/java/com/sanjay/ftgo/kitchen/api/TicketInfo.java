package com.sanjay.ftgo.kitchen.api;

import java.time.ZonedDateTime;

public record TicketInfo(Long id, Long orderId, String status, ZonedDateTime readyBy) {
}
