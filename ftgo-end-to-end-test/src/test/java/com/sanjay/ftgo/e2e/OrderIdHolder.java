package com.sanjay.ftgo.e2e;

// Shared per-scenario state: Cucumber-Java instantiates one instance of each step-definition
// class per scenario, sharing a single PicoContainer DI context — a plain field on one
// step-definition class isn't visible to another, so this holder bean bridges orderId between
// PlaceReviseCancelOrderStepDefinitions (producer) and OrderAccessControlStepDefinitions (consumer).
public class OrderIdHolder {
    private long orderId;
    public void set(long orderId) { this.orderId = orderId; }
    public long get() { return orderId; }
}
