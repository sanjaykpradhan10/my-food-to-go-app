package com.sanjay.ftgo.order.componenttest;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class OrderServiceComponentTestStepDefinitions {

    private SagaParticipantStub sagaParticipantStub;

    @Before
    public void setUp() {
        sagaParticipantStub = new SagaParticipantStub("localhost:9092");
    }

    @After
    public void tearDown() {
        sagaParticipantStub.close();
    }

    @Given("the Restaurant Service stub is serving restaurant {int} with menu item {int} priced at {double}")
    public void theRestaurantServiceStubIsServing(int restaurantId, int menuItemId, double price) {
        throw new io.cucumber.java.PendingException();
    }

    @Given("the saga participant stub will approve the accounting authorization")
    public void theSagaParticipantStubWillApprove() {
        sagaParticipantStub.setAccountingShouldApprove(true);
    }

    @Given("the saga participant stub will decline the accounting authorization")
    public void theSagaParticipantStubWillDecline() {
        sagaParticipantStub.setAccountingShouldApprove(false);
    }

    @When("a consumer places an order for {int} of menu item {int} from restaurant {int}")
    public void aConsumerPlacesAnOrder(int quantity, int menuItemId, int restaurantId) {
        throw new io.cucumber.java.PendingException();
    }

    @Then("the order is eventually approved")
    public void theOrderIsEventuallyApproved() {
        throw new io.cucumber.java.PendingException();
    }

    @Then("the order is eventually rejected")
    public void theOrderIsEventuallyRejected() {
        throw new io.cucumber.java.PendingException();
    }
}
