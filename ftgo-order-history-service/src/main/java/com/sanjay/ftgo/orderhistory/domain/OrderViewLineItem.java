package com.sanjay.ftgo.orderhistory.domain;

import jakarta.persistence.Embeddable;

@Embeddable
public record OrderViewLineItem(Long menuItemId, int quantity) {
}
