package com.sanjay.ftgo.order.domain;

import jakarta.persistence.Embeddable;

@Embeddable
public record OrderLineItem(Long menuItemId, int quantity) {
}
