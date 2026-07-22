package com.sanjay.ftgo.order.eventsourcing;

import com.sanjay.ftgo.order.domain.OrderRevision;

public record ReviseOrderCommand(OrderRevision revision) implements OrderCommand {
}
