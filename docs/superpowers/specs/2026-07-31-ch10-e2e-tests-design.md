# Ch.10 Sub-Project 3: End-to-End Tests — Design

**Status:** Approved
**Scope:** Chapter 10 of *Microservices Patterns* (§10.3, pp.345-347), sub-project 3 of 3 (end-to-end tests). Sub-project 1 (consumer-driven contract tests) shipped in PR #21. Sub-project 2 (component tests) shipped in PR #22. This is the last of Ch.10's three sub-projects.

## Goal

Add a single Gherkin/Cucumber "user journey" end-to-end test, per the book's own §10.3 example (`ftgo-end-to-end-test` module, Listing 10.17): the *entire* FTGO application runs for real (all business services, both gateways, and infrastructure), and one scenario drives a consumer through Create → Revise → Cancel Order, entered through the public API gateway exactly as a real client would.

## Why full stack, no stubs

Sub-project 2's component test deliberately stubbed everything outside order-service (Restaurant Service via WireMock, saga participants via a Kafka stub) to test one service in isolation — the book's own distinction between component and end-to-end testing (§10.3: "Component testing tests each service separately. End-to-end testing, though, tests the entire application."). This sub-project is the complementary top-of-the-pyramid test: it deliberately runs everything for real, including full saga participant behavior and gateway routing, and is intentionally the only test at this level. Per the book's own guidance (§10.3.1), a *single* user-journey scenario is used, not one test per operation, precisely because end-to-end tests are slow and brittle and should be minimized.

## Prerequisite: creation endpoints

**Problem found during audit:** neither restaurant-service nor consumer-service has a REST endpoint to create data — both only seed fixed fixtures via a `DataSeeder` (`CommandLineRunner`) on startup: restaurant-service seeds "Ajanta Indian Cuisine" (id 1) and "Pizza Palace" (id 2); consumer-service seeds "Sanjay" (active) and "Blocked Consumer" (inactive). A real end-to-end user journey needs to create its own data rather than depend on hardcoded seed IDs, so this sub-project adds the missing endpoints as a small prerequisite.

- **`POST /restaurants`** on restaurant-service. Request: `{"name": string, "menuItems": [{"name": string, "price": number}]}`. Response: `RestaurantResponse` (existing DTO — `{id, name, menuItems: [{id, name, price}]}`), id auto-generated (`GenerationType.IDENTITY`, unchanged from today).
- **`POST /consumers`** on consumer-service (no `ConsumerController` exists today — this is a new class). Request: `{"name": string, "active": boolean}`. Response: `{id, name, active}`.
- `DataSeeder` in both services is untouched — it keeps seeding its fixed fixtures on startup, since sub-project 2's component test and manual `docker compose up` runs depend on that seeded data continuing to exist.
- Neither new endpoint is exposed through `ftgo-public-gateway` — the gateway's route config has no `/api/v1/consumers/**` route today, and creating restaurants/menus isn't a public-facing operation in this application's design. The e2e test calls both endpoints directly against their service ports (8085 restaurant-service, 8081 consumer-service) inside the compose network, not through a gateway.

## Journey scenario

One Gherkin scenario, chaining all three saga types into a single user journey (§10.3.1: "rather than test create order, revise order, and cancel order separately, you can write a single test that does all three"):

```gherkin
Feature: Place, Revise, and Cancel Order (end-to-end)

  Scenario: A consumer places, revises, and cancels an order
    Given a restaurant "Ajanta E2E" with a menu item "Chicken Vindaloo" priced at 12.00
    And an active consumer "E2E Consumer"
    When the consumer places an order for 2 of the menu item at the restaurant
    Then the order is eventually approved
    When the consumer revises the order to 12 of the menu item
    Then the order is eventually rejected
    When the consumer cancels the order
    Then the order is eventually cancelled
```

**Approve/decline mechanism:** accounting-service's `SagaJoinService.isAuthorized(totalQuantity)` approves iff total line-item quantity ≤ `AUTHORIZATION_QUANTITY_LIMIT` (10) — this is the only decline trigger that exists anywhere in this codebase (there is no card-expiry or amount-based sentinel). The scenario places an order with quantity 2 (approves), then revises to quantity 12 (declines), exercising both the CreateOrder saga's approval path and the ReviseOrder saga's rejection path in one flow, followed by a CancelOrder saga to close out the journey. This is a deliberate adaptation of the book's own example (which uses an "expired credit card" framing not present in this codebase's domain logic) to the actual mechanism this application implements.

**Entry point:** every order operation (create, poll status, revise, cancel) goes through `ftgo-public-gateway` at `http://localhost:8091/api/v1/orders/...` with header `X-Api-Key: public-dev-key` — the gateway's `public-orders` route wildcard-rewrites `/api/v1/orders/**` to `/orders**` on order-service, confirmed to cover the `/{id}/revise` and `/{id}/cancel` sub-paths. The restaurant/consumer setup calls bypass the gateway (see above).

**Saga mode:** `SAGA_MODE=orchestration` (root `compose.yml` default is `choreography`) — consistent with sub-project 2 and the book's own orchestration-based worked examples for these three sagas.

**Persistence mode:** `PERSISTENCE_MODE=jpa` (root `compose.yml`'s existing default) — event-sourced mode is out of scope.

## Infrastructure & tooling

- **Reuse the existing root `compose.yml` unmodified** — it already defines the full stack: `mysql`, `zookeeper`, `kafka`, `kafka-connect`/`connector-registrar` (Debezium, only active in CDC outbox mode — default `polling` mode keeps these two containers no-ops), `service-registry`, all seven business services, and both gateways (`mobile-gateway`, `public-gateway`). No new compose file. The test invocation overrides `SAGA_MODE=orchestration` as an environment variable at compose-up time (the file's other env vars already default sanely: `OUTBOX_PUBLISH_MODE=polling`, `PERSISTENCE_MODE=jpa`).
- **New Gradle module `ftgo-end-to-end-test`**, matching the book's own module name for this exact concern, added to `settings.gradle`.
- **`com.avast.gradle.docker-compose` Gradle plugin** (already a project dependency from sub-project 2) configured against the root `compose.yml` with the `SAGA_MODE` override, wired into the module's `test` task: brings the stack up before tests, tears it down after — pass or fail — same lifecycle already established in sub-project 2.
- **Cucumber** (JUnit Platform engine), same as sub-projects 1 (contract verifier feature files are Spring Cloud Contract, not Cucumber — sub-project 2 is the first and only prior Cucumber usage) and 2.
- **Readiness:** this stack is heavier than sub-project 2's five-container slice (9 services + infra, each building its own image and self-registering with Eureka). Step definitions retry the initial restaurant-creation call with a bounded backoff (e.g. up to 60s) before failing, to absorb slow-starting service registration rather than requiring a separate explicit health-check step in the compose config.

## File layout

- `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/RestaurantController.java` — add `POST /restaurants` handler (or a sibling controller if that keeps the file focused — implementer's call, follows existing package conventions)
- `ftgo-restaurant-service/src/main/java/com/sanjay/ftgo/restaurant/api/CreateRestaurantRequest.java` (new)
- `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/api/ConsumerController.java` (new package `api`, doesn't exist yet in this service)
- `ftgo-consumer-service/src/main/java/com/sanjay/ftgo/consumer/api/CreateConsumerRequest.java`, `ConsumerResponse.java` (new)
- `settings.gradle` — add `include 'ftgo-end-to-end-test'`
- `ftgo-end-to-end-test/build.gradle` — new module: Cucumber deps, docker-compose plugin config pointed at root `compose.yml`
- `ftgo-end-to-end-test/src/test/resources/features/PlaceReviseCancelOrder.feature`
- `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/EndToEndTestRunner.java` — Cucumber JUnit Platform runner
- `ftgo-end-to-end-test/src/test/java/com/sanjay/ftgo/e2e/PlaceReviseCancelOrderStepDefinitions.java`

## Deferred to a future sub-project

- Choreography-mode end-to-end coverage (this sub-project covers orchestration only, consistent with sub-project 2's deferral)
- Event-sourced persistence mode (`PERSISTENCE_MODE=eventsourcing`) end-to-end coverage
- Additional user journeys beyond Create/Revise/Cancel (e.g. courier assignment, delivery completion) — out of scope per §10.3.1's guidance to minimize the number of end-to-end tests

## Testing

- `./gradlew :ftgo-end-to-end-test:test` — brings up the full compose stack (image builds for all 9 services), runs the one Cucumber scenario, tears down. Expect this to take noticeably longer than sub-project 2's component test (book quotes 4-5 minutes for a lighter stack; this stack has roughly double the containers).
- `./gradlew :ftgo-restaurant-service:test :ftgo-consumer-service:test` — unit/slice-level coverage for the two new controllers, run as part of the normal `test` task (fast, no Docker).
