Feature: Order access control

  Scenario: A consumer can fetch their own order
    Given a restaurant "Ajanta" with a menu item "Chicken Curry" priced at 12.50
    And an active consumer "Alice"
    When the consumer places an order for 2 of the menu item at the restaurant
    Then the consumer can fetch their own order

  Scenario: A consumer cannot fetch another consumer's order
    Given a restaurant "Ajanta" with a menu item "Chicken Curry" priced at 12.50
    And an active consumer "Alice"
    When the consumer places an order for 2 of the menu item at the restaurant
    Then a different consumer is forbidden from fetching the order

  Scenario: A request with no Authorization header is rejected at the gateway
    Given a restaurant "Ajanta" with a menu item "Chicken Curry" priced at 12.50
    And an active consumer "Alice"
    When the consumer places an order for 2 of the menu item at the restaurant
    Then fetching the order with no Authorization header returns 401

  Scenario: A request with a malformed token is rejected at the gateway
    Given a restaurant "Ajanta" with a menu item "Chicken Curry" priced at 12.50
    And an active consumer "Alice"
    When the consumer places an order for 2 of the menu item at the restaurant
    Then fetching the order with a malformed token returns 401
