package com.sanjay.ftgo.kitchen.api;

import java.time.ZonedDateTime;

public record AcceptTicketRequest(ZonedDateTime readyBy) {
}
