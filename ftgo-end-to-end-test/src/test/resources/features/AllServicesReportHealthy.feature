Feature: All services report healthy (end-to-end)

  Scenario: Every FTGO service's health endpoint reports UP
    Then every service's health endpoint eventually reports UP
