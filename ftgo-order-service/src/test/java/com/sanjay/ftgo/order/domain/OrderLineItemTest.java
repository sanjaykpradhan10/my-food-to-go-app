package com.sanjay.ftgo.order.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderLineItemTest {

    @Test
    void exposesMenuItemIdAndQuantity() {
        OrderLineItem lineItem = new OrderLineItem(10L, 3);

        assertThat(lineItem.menuItemId()).isEqualTo(10L);
        assertThat(lineItem.quantity()).isEqualTo(3);
    }

    @Test
    void twoLineItemsWithTheSameValuesAreEqual() {
        OrderLineItem a = new OrderLineItem(10L, 3);
        OrderLineItem b = new OrderLineItem(10L, 3);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void lineItemsDifferingByMenuItemIdAreNotEqual() {
        OrderLineItem a = new OrderLineItem(10L, 3);
        OrderLineItem b = new OrderLineItem(11L, 3);

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void lineItemsDifferingByQuantityAreNotEqual() {
        OrderLineItem a = new OrderLineItem(10L, 3);
        OrderLineItem b = new OrderLineItem(10L, 4);

        assertThat(a).isNotEqualTo(b);
    }
}
