package com.sanjay.ftgo.order.contracttest;

import com.sanjay.ftgo.order.domain.Order;
import com.sanjay.ftgo.order.domain.OrderLineItem;
import com.sanjay.ftgo.order.domain.OrderStatus;

import java.util.List;

// Both HttpBase (Task 2's GET /orders/{id} contract) and MessagingBase (Task 4's orderCreated
// contract) build the same Order fixture and reference the same orderId - kept in one place so
// the two contracts can't silently drift apart on what "order 1223232" looks like.
final class ContractFixtures {

    static final long ORDER_ID = 1223232L;

    private ContractFixtures() {
    }

    static Order sampleOrder() {
        return new Order(ORDER_ID, 1L, 1L,
                List.of(new OrderLineItem(10L, 2)), OrderStatus.APPROVAL_PENDING);
    }
}
