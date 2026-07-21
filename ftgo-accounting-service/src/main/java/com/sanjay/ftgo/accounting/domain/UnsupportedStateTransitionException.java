package com.sanjay.ftgo.accounting.domain;

public class UnsupportedStateTransitionException extends RuntimeException {

    public UnsupportedStateTransitionException(AuthorizationStatus status) {
        super("Unsupported transition from status " + status);
    }
}
