Feature: Place, Revise, and Cancel Order (end-to-end)

  Scenario: A consumer places, revises, and cancels an order
    Given a restaurant "Ajanta E2E" with a menu item "Chicken Vindaloo" priced at 12.00
    And an active consumer "E2E Consumer"
    When the consumer places an order for 2 of the menu item at the restaurant
    Then the order is eventually approved
    When the consumer revises the order to 12 of the menu item
    Then the revision is eventually declined and the order keeps its original quantity of 2
    When the consumer cancels the order
    Then the order is eventually cancelled

  Scenario: Placing and approving an order increments the order-service Prometheus counters
    Given a restaurant "Ajanta Metrics E2E" with a menu item "Lamb Biryani" priced at 14.00
    And an active consumer "Metrics E2E Consumer"
    When the consumer places an order for 1 of the menu item at the restaurant
    Then the order is eventually approved
    And the order-service Prometheus counters "orders_placed_total" and "orders_approved_total" both eventually read at least 1

  Scenario: Placing and approving an order produces a single trace spanning multiple services
    Given a restaurant "Ajanta Tracing E2E" with a menu item "Lamb Biryani" priced at 14.00
    And an active consumer "Tracing E2E Consumer"
    When the consumer places an order for 1 of the menu item at the restaurant
    Then the order is eventually approved
    And Tempo eventually has a trace for "ftgo-public-gateway" spanning at least 2 distinct services

  Scenario: Live config refresh changes the outbox polling interval without a restart
    Given a restaurant "Ajanta Config E2E" with a menu item "Butter Chicken" priced at 13.00
    And an active consumer "Config E2E Consumer"
    And the outbox poll interval for ftgo-order-service is set to 2000 milliseconds via the config repo
    When I place an order and measure the outbox publish delay
    Then the measured outbox publish delay is close to 2000 milliseconds
    When I set the outbox poll interval for ftgo-order-service to 300 milliseconds via the config repo
    And I refresh the configuration for ftgo-order-service
    Then the order-service outbox poll interval reported by actuator is 300 milliseconds
    And I place another order and measure the outbox publish delay
    Then the measured outbox publish delay is close to 300 milliseconds

  Scenario: Triggering an uncaught exception reports it to GlitchTip
    When an admin triggers the order-service diagnostic exception endpoint
    Then the diagnostic endpoint responds with a server error
    And GlitchTip eventually reports an IllegalStateException issue for ftgo-order-service

  Scenario: Placing an order records an audit log entry
    Given a restaurant "Ajanta Audit E2E" with a menu item "Tandoori Chicken" priced at 15.00
    And an active consumer "Audit E2E Consumer"
    When the consumer places an order for 1 of the menu item at the restaurant
    Then the order is eventually approved
    And the audit log eventually has an entry for the placed order with action containing "createOrder"
