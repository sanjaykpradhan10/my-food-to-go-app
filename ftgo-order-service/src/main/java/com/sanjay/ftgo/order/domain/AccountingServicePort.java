package com.sanjay.ftgo.order.domain;

public interface AccountingServicePort {

    SectionResult<AuthorizationInfo> findAuthorization(Long orderId);
}
