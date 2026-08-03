package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.Order;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.List;

// Shared by OrderController and OrderViewController: both endpoints return order-identifying
// data (consumerId), so both need the identical instance-based ACL - ADMIN unconditionally,
// or CONSUMER only for their own order. Extracted here rather than duplicated because this is
// real, immediate duplication in the same module, not a hypothetical future one.
final class OrderAccessControl {

    private OrderAccessControl() {
    }

    static void enforce(Order order, Jwt jwt) {
        List<String> roles = jwt.getClaimAsStringList("roles");
        if (roles != null && roles.contains("ADMIN")) {
            return;
        }
        if (roles != null && roles.contains("CONSUMER") && jwt.getSubject().equals(String.valueOf(order.getConsumerId()))) {
            return;
        }
        throw new AccessDeniedException("Not authorized to view order " + order.getId());
    }
}
