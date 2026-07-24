package com.sanjay.ftgo.order.api;

import com.sanjay.ftgo.order.domain.AuthorizationInfo;
import com.sanjay.ftgo.order.domain.DeliveryInfo;
import com.sanjay.ftgo.order.domain.RestaurantInfo;
import com.sanjay.ftgo.order.domain.SectionResult;
import com.sanjay.ftgo.order.domain.TicketInfo;

public record OrderViewResponse(
        OrderSummary order,
        SectionResult<RestaurantInfo> restaurant,
        SectionResult<TicketInfo> ticket,
        SectionResult<AuthorizationInfo> authorization,
        SectionResult<DeliveryInfo> delivery) {
}
