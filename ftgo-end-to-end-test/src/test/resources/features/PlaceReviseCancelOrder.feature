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
