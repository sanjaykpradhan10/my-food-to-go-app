Feature: Place Order

  Background:
    Given the Restaurant Service stub is serving restaurant 1 with menu item 1 priced at 12.50

  Scenario: Order authorized
    Given the saga participant stub will approve the accounting authorization
    When a consumer places an order for 2 of menu item 1 from restaurant 1
    Then the order is eventually approved

  Scenario: Order rejected due to expired credit card
    Given the saga participant stub will decline the accounting authorization
    When a consumer places an order for 2 of menu item 1 from restaurant 1
    Then the order is eventually rejected
