package com.sanjay.ftgo.orderhistory.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderViewTest {

    @Test
    void newOrderViewStartsWithNullStatusesAndEmptyLineItems() {
        OrderView view = new OrderView(42L);

        assertThat(view.getOrderId()).isEqualTo(42L);
        assertThat(view.getOrderStatus()).isNull();
        assertThat(view.getTicketStatus()).isNull();
        assertThat(view.getAuthorizationStatus()).isNull();
        assertThat(view.getDeliveryStatus()).isNull();
        assertThat(view.getCourierId()).isNull();
        assertThat(view.getLineItems()).isEmpty();
    }

    @Test
    void settersUpdateFields() {
        OrderView view = new OrderView(42L);

        view.setConsumerId(1L);
        view.setRestaurantId(7L);
        view.setOrderStatus("APPROVAL_PENDING");
        view.setLineItems(List.of(new OrderViewLineItem(10L, 2)));

        assertThat(view.getConsumerId()).isEqualTo(1L);
        assertThat(view.getRestaurantId()).isEqualTo(7L);
        assertThat(view.getOrderStatus()).isEqualTo("APPROVAL_PENDING");
        assertThat(view.getLineItems()).containsExactly(new OrderViewLineItem(10L, 2));
    }
}
