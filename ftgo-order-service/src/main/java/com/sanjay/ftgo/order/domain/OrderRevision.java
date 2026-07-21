package com.sanjay.ftgo.order.domain;

import java.util.List;

public record OrderRevision(List<OrderLineItem> revisedLineItems) {
}
