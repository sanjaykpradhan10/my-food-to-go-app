package com.sanjay.ftgo.order.domain;

public interface KitchenServicePort {

    SectionResult<TicketInfo> findTicket(Long orderId);
}
