package com.sanjay.ftgo.accounting.domain;

public sealed interface AuthorizationDomainEvent
        permits CardAuthorizedEvent, CardAuthorizationDeclinedEvent, AuthorizationReversedEvent,
                AuthorizationRevisedEvent, AuthorizationRevisionRejectedEvent {

    Long orderId();
}
