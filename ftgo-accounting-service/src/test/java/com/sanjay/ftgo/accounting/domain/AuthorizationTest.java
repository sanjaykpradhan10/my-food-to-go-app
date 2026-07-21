package com.sanjay.ftgo.accounting.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuthorizationTest {

    @Test
    void authorizeStartsInAuthorizedAndEmitsCardAuthorized() {
        AuthorizationResult result = Authorization.authorize(42L);

        assertThat(result.authorization().getStatus()).isEqualTo(AuthorizationStatus.AUTHORIZED);
        assertThat(result.authorization().getOrderId()).isEqualTo(42L);
        assertThat(result.events()).containsExactly(new CardAuthorizedEvent(42L));
    }

    @Test
    void declineStartsInDeclinedAndEmitsCardAuthorizationDeclined() {
        AuthorizationResult result = Authorization.decline(42L, "order quantity exceeds authorization limit");

        assertThat(result.authorization().getStatus()).isEqualTo(AuthorizationStatus.DECLINED);
        assertThat(result.events()).containsExactly(
                new CardAuthorizationDeclinedEvent(42L, "order quantity exceeds authorization limit"));
    }

    @Test
    void reverseMovesFromAuthorizedToReversed() {
        Authorization authorization = Authorization.authorize(42L).authorization();

        List<AuthorizationDomainEvent> events = authorization.reverse();

        assertThat(authorization.getStatus()).isEqualTo(AuthorizationStatus.REVERSED);
        assertThat(events).containsExactly(new AuthorizationReversedEvent(42L));
    }

    @Test
    void reverseFromDeclinedThrows() {
        Authorization authorization = Authorization.decline(42L, "reason").authorization();

        assertThatThrownBy(authorization::reverse).isInstanceOf(UnsupportedStateTransitionException.class);
    }

    @Test
    void reverseFromAlreadyReversedThrows() {
        Authorization authorization = Authorization.authorize(42L).authorization();
        authorization.reverse();

        assertThatThrownBy(authorization::reverse).isInstanceOf(UnsupportedStateTransitionException.class);
    }
}
