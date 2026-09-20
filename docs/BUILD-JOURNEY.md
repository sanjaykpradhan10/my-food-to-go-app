# The FTGO Build Journey

A plain-language, chapter-by-chapter walkthrough of how this project was built, following the book *Microservices Patterns* by Chris Richardson (the "FTGO" - Food To Go - example application the book itself is built around).

## What this document is for

`README.md` tells you what the system *is* today.
`docs/ARCHITECTURE.md` is a technical reference - diagrams, sequence flows, exact class names.
This document is different: it's a **story**.
It walks through the book one chapter at a time, in the order both the book presents its ideas and this project actually built them, and for each chapter answers three questions in plain English:

1. What is the book's idea, in terms anyone can follow?
2. What's a concrete example of that idea?
3. What did this project actually build to put that idea into practice?

Read it top to bottom like a book. Each section links out to `docs/ARCHITECTURE.md` or another reference doc if you want the full technical detail behind a summary here.

A couple of acronyms come up constantly, so here they are once, up front:

- **DDD** - Domain-Driven Design: designing your code's structure around the real business concepts (Order, Ticket, Delivery) rather than around technical layers.
- **CQRS** - Command Query Responsibility Segregation: using a different model for writing data than for reading it.
- **BFF** - Backend for Frontend: a small API gateway built for one specific kind of client (e.g. the mobile app).
- **CDC** - Change Data Capture: watching a database's transaction log to notice changes, instead of the application explicitly announcing them.
- **mTLS** - mutual TLS: both sides of a connection prove their identity with a certificate, not just the server.
- **JWT** - JSON Web Token: a signed, tamper-proof blob of claims (like "this is user 42, role CONSUMER") that a server can trust without a database lookup.

---

## What is FTGO?

FTGO is a food delivery application - think "order food from a restaurant, a courier picks it up and delivers it." It's the book's running example precisely because it's simple to describe but has enough moving parts (placing an order, paying for it, preparing it, delivering it) to need *real* microservices patterns, not toy ones. This project builds FTGO from scratch, chapter by chapter, as a way to learn those patterns by doing rather than just reading.

---

## Chapter 1 - Escaping monolithic hell

**The book's idea.** A single, growing codebase ("a monolith") eventually becomes hard to work with: every change risks breaking something unrelated, the whole thing has to be redeployed for a one-line fix, and different parts of the system that want different technology stacks are stuck sharing one. The book calls this "monolithic hell." Its proposed fix is to split the system along its natural business boundaries into small, independently deployable services - microservices - each owning its own data.

**A concrete example.** Imagine one giant `FoodDeliveryApplication` where the code for taking orders, the code for tracking restaurant menus, and the code for paying couriers all live in the same process and share one database table for "everything." A bug in the payment code can't be fixed without redeploying (and re-testing) the entire order-taking flow too. Splitting that into an Order Service, a Restaurant Service, and an Accounting Service - each with its own database - means a payment bug fix touches only the Accounting Service.

**What was actually built.** This chapter is conceptual: there's no monolith in this codebase to escape, because the project was built as microservices from day one. The chapter was read and its concepts (the "scale cube" - three axes along which you can scale a system, and *why* microservices is specifically a "Y-axis" split by function) are the mental model behind every later architectural decision, even though no code came out of this chapter directly.

---

## Chapter 2 - Decomposition strategies

**The book's idea.** If microservices means "split the system," the hard question is *where* to draw the lines. The book gives two complementary techniques: decompose by **business capability** (what does the business do? - "manage orders," "manage deliveries," "manage payments") and decompose by **DDD subdomain** (what are the distinct areas of knowledge, each with its own vocabulary?). It also introduces the idea of a **bounded context**: a boundary inside which a term means one specific thing, because the same real-world word can mean different things to different parts of the business.

**A concrete example.** The word "Order" means different things in different places: to the kitchen it's a **Ticket** (what to cook, in what sequence); to the courier it's a **Delivery** (where to pick up, where to drop off); to accounting it's an **Authorization** (how much to charge, whether the charge succeeded). Each of those is its own bounded context with its own model, even though a person outside the system would call all of them "the order."

**What was actually built.** Again conceptual - no code - but this is the chapter that explains *why* this project ended up with separate `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-delivery-service`, and `ftgo-accounting-service` modules instead of one big "orders" module: each one is a different bounded context, matching the book's own Ticket/Delivery/Authorization split almost exactly.

---

## Chapter 3 - Interprocess communication

**The book's idea.** Once services are separate processes, they need to talk to each other, and there's more than one way to do it. The book covers several styles: synchronous request/response (one service calls another and waits for the answer, e.g. over REST), asynchronous messaging (a service publishes an event and moves on, without waiting for anyone to react), the **circuit breaker** pattern (stop calling a service that's clearly failing, instead of retrying into a pile of slow timeouts), **service discovery** (how does a caller find *where* another service currently is, given that instances come and go), and the **transactional outbox** (how do you reliably publish "I changed my data" *and* commit that change to your database, as a single atomic unit, when a database transaction and a message broker are two separate systems that can't be updated together)?

**A concrete example.** `ftgo-order-service` needs restaurant details to build an order. A naive synchronous call (`GET /restaurants/{id}`) works until restaurant-service is slow or down - then every order attempt hangs waiting on it too. Wrapping that call in a circuit breaker means: after enough recent failures, stop calling restaurant-service for a while and fail fast instead, so order-service stays responsive even when its dependency isn't. Separately, when order-service creates an Order, it also needs to tell the rest of the system "a new order was created" - but if it writes the Order row to its database and *then* tries to publish to Kafka, a crash between those two steps means a saved order nobody else ever hears about. The transactional outbox fixes this by writing the "event to publish" as a row in the *same database transaction* as the Order itself, so both succeed together or neither does; a background poller then reads that table and actually publishes to Kafka.

**What was actually built.**

- Synchronous REST + circuit breaker: `order-service` calls `restaurant-service`'s `GET /restaurants/{id}` through a Resilience4j circuit breaker.
- Asynchronous messaging + transactional outbox: `order-service` writes an `OrderCreated` event to an outbox table in the same transaction as the Order row; a scheduled poller publishes it to the `order.events` Kafka topic, which `kitchen-service` consumes.
- Service discovery: a standalone Eureka server (`ftgo-service-registry`), used by every service to find each other by name instead of hardcoded addresses.
- Transaction log tailing (CDC): explored as an alternative outbox-publishing mechanism (reading the database's own change log instead of polling a table).

See `docs/ARCHITECTURE.md`'s "The transactional outbox pattern" and "Kafka topic catalog" sections for the full technical picture.

---

## Chapter 4 - Managing transactions with sagas

**The book's idea.** Placing an order touches multiple services (reserve credit, notify the kitchen, arrange delivery) - each with its own database. A traditional multi-service ACID transaction (lock everything, commit or roll back together) doesn't scale across independently owned databases. A **saga** is the alternative: break the operation into a sequence of local transactions, each in one service, where every step that can fail has a matching *compensating* step to undo the effects of the steps that already succeeded. Sagas can be coordinated two ways: **choreography** (each service reacts to events published by the previous one - no central coordinator) or **orchestration** (a dedicated saga orchestrator tells each participant what to do next, step by step).

**A concrete example.** Placing an order: 1) order-service creates an Order in `PENDING` state, 2) accounting-service reserves credit, 3) kitchen-service creates a Ticket, 4) order-service marks the Order `APPROVED`. If step 2 fails (insufficient credit), there's no step 3 or 4 to run - the saga instead runs a compensating action: order-service marks the Order `REJECTED`. Nothing was ever locked across services; each step was its own local, committed transaction, and failure was handled by *moving forward* with a corrective action, not by rolling back time.

**What was actually built.** The **Create Order saga**, implemented *both* ways so the two coordination styles could be compared directly rather than just read about, switchable at runtime via a `SAGA_MODE` environment variable. In choreography mode, each service publishes a domain event and the next participant reacts to it; in orchestration mode, a `CreateOrderSagaOrchestrator` in order-service explicitly issues each command and reacts to each reply. Both reach the identical outcome for the same inputs. See `docs/ARCHITECTURE.md`'s "Create Order saga - choreography" and "- orchestration" sections for the full step-by-step sequence diagrams.

---

## Chapter 5 - Designing business logic

**The book's idea.** Where should the rules that govern business behavior actually live? The book contrasts **transaction script** (a service class with one procedural method per operation, with no real object model behind it) against a proper **domain model**, where behavior lives on the entities themselves. **DDD (Domain-Driven Design)** is a refinement of the domain model approach, organized around **aggregates**: a cluster of objects (like an Order and its line items) treated as one consistency boundary, always modified together, with one designated **aggregate root** that's the only object outside code is allowed to reference directly. Three rules keep aggregates well-behaved: only reference the aggregate root from outside; between aggregates, reference by primary key (never hold a direct object reference to another aggregate); and one transaction should create or update exactly one aggregate - which is *precisely why sagas (Chapter 4) exist at all*, since an operation spanning multiple aggregates can't be one local transaction.

**A concrete example.** Before the refactor, `TicketService`/`SagaJoinService` were transaction scripts: a bag of procedural methods mutating raw fields with no guarded state machine. After the refactor, `Order` is a proper aggregate: it has real methods like `revise()` and `confirmRevision()`, its own state machine (you can't jump straight from `PENDING` to `DELIVERED`), and code elsewhere refers to a Consumer only by `consumerId` (a plain primary key), never by holding an actual `Consumer` object.

**What was actually built.** All three of this project's DDD aggregates - `Ticket` (kitchen-service), `Order` (order-service), and `Authorization` (accounting-service) - were refactored into proper aggregates with enforced state transitions and domain events returned from their own methods, going one step beyond the book's own two named worked examples (`Ticket` and `Order`) by applying the same rigor to `Authorization` as well.

---

## Chapter 6 - Event sourcing

**The book's idea.** Instead of storing an aggregate's *current* state as a row you overwrite, event sourcing stores every state-changing event that ever happened to it, forever, in an append-only log - and rebuilds current state by replaying those events. This turns the event store into a hybrid of a database (it's your durable system of record) and a message broker (via CDC on that same log, it's also how other services find out what happened). The discipline that makes this work is a strict split: a `process(Command)` step decides *what happened* (validates input, doesn't mutate anything), and a separate `apply(Event)` step unconditionally mutates state given something that already happened - the same `apply()` runs both for a just-decided event and for every historical event during a replay. **Snapshots** (a saved current-state checkpoint) are purely a performance shortcut, so you don't replay years of events every time - they don't change correctness.

**A concrete example.** Instead of a single `orders` table row that just says `status = APPROVED`, the event store holds a sequence like `OrderCreated`, `OrderAuthorized`, `TicketCreated` - and `Order`'s current state is whatever you get by replaying those in order. Rebuild the same Order from scratch tomorrow, replay the same events, get the identical result - that replayability is the whole point.

**What was actually built.** A hand-rolled event-sourced persistence path for the `Order` aggregate: an `OrderEventStore`/`OrderAggregate` pair implementing the `process`/`apply` split, a snapshot mechanism, and a dedicated optimistic-lock version table (rather than deriving the version by counting rows), switchable against the regular JPA persistence path via a `PERSISTENCE_MODE` flag. It covers all three `Order` sagas (create, cancel, revise) in both saga styles, including the book's full orchestration-mode mechanism for publishing saga commands as pseudo-events. See `docs/ARCHITECTURE.md`'s "Event sourcing - `Order` aggregate (Ch.6)" section for the detailed mechanics.

---

## Chapter 7 - Implementing queries

**The book's idea.** Once data is spread across several services' own databases, how does a client get a single view that spans more than one of them? The book presents two genuinely different answers, not two versions of the same idea: **API composition** (assemble the response at request time, by calling each service live and combining the results) and **CQRS** (maintain a separate, pre-built read model ahead of time, kept up to date by consuming the same domain events every service already publishes).

**A concrete example.** A customer wants to see their order's full status: the order itself, the restaurant's name, the kitchen's prep status, the payment status, and the courier's location. API composition means calling all four services in parallel right now and stitching the results together - always current, but only as available as the least available of the four, and only as fast as the slowest one. CQRS means a separate service has already been listening to every relevant event as it happened and keeps a single, ready-to-read row per order - instant to read, decoupled from those four services' availability, but only as fresh as the last event it processed (a moment behind reality, not a live snapshot).

**What was actually built.** Both, deliberately, to see the trade-off firsthand rather than just read about it:

- **API composition**: `GET /orders/{id}/view` on order-service, fanning out to restaurant/kitchen/accounting/delivery-service in parallel via virtual threads, each guarded by its own circuit breaker; a `SectionResult` type (`Found`/`NotFound`/`Unavailable`) means one slow or down dependency degrades only its own section of the response, not the whole thing.
- **CQRS**: a new standalone `ftgo-order-history-service` maintains a denormalized `order_views` read model by consuming events from every relevant Kafka topic and upserting; it has no Eureka registration and answers `GET /order-views/{orderId}` with zero synchronous calls to anything, since its own availability must never depend on - or be depended on by - the write-side services.

See `docs/ARCHITECTURE.md`'s "API composition" and "CQRS" sections for the sequence diagrams.

---

## Chapter 8 - External API patterns

**The book's idea.** Client applications (a mobile app, a public partner API) shouldn't talk directly to a dozen backend services - there needs to be a single front door. The book presents this as one underlying pattern (an **API gateway**: an edge service that routes, and sometimes composes, requests to the services behind it) applied with two different ownership choices: one shared gateway for everyone, or a **Backend for Frontend (BFF)** - a separate gateway per client type, each independently owned and configured for that client's specific needs.

**A concrete example.** A mobile app typically wants a few tailored, composed endpoints (minimize round trips over a slow connection); a public partner API typically wants broad, generic routes with its own API-key and rate-limit rules. Giving them the same gateway means every change has to satisfy both sets of needs at once; giving each its own gateway lets the mobile team ship changes without coordinating with the partner-API team.

**What was actually built.** The BFF variant: `ftgo-mobile-gateway` (routing plus one hand-composed endpoint, `GET /mobile/orders/{orderId}`) and `ftgo-public-gateway` (pure routing to six backend services), each independently configured (its own API key, its own rate limit), sharing common edge behavior (request logging, API-key auth) via a shared `ftgo-gateway-common` library. A real, non-obvious finding from building this: Spring Cloud Gateway's filter chain only applies to requests matched by a declared route - a hand-written WebFlux endpoint in the same application is dispatched by a completely different mechanism and never sees that filter chain, so cross-cutting concerns (auth, logging) have to be added by hand on such endpoints. See `docs/ARCHITECTURE.md`'s "API Gateway / Backends for Frontends" section.

---

## Chapter 9 - Testing microservices: Part 1

**The book's idea.** The **test pyramid** says: many fast unit tests at the base, fewer slower integration-style tests above that, and very few full end-to-end tests at the top - because end-to-end tests are valuable but slow and brittle, while unit tests are cheap and should catch most bugs. Within unit testing, the book distinguishes **sociable** tests (test a class together with its real, cheap collaborators - appropriate for entities, value objects, and sagas) from **solitary** tests (mock the collaborators and test just this class's own logic - appropriate for domain services, controllers, and message handlers).

**A concrete example.** Testing the `Order` aggregate's `revise()` method by constructing a real `Order` through its own methods and asserting on its resulting state is a *sociable* test - there's nothing worth mocking, the "collaborator" is just the aggregate's own cheap-to-build state. Testing a controller that calls out to `OrderService` is a *solitary* test - you mock `OrderService` and assert the controller called it correctly, without actually running real business logic underneath.

**What was actually built.** This project's existing test suite was audited against the book's six named unit-testing techniques and already matched four of them independently - evidence that the DDD-aggregate work from Chapter 5 had already been quietly shaping the tests toward this structure. The two real gaps found were about assertion *depth*, not technique: several saga-orchestrator and message-handler tests checked that a call happened but not that its payload was correct - tightened across the relevant test classes, plus a new standalone value-object test for `OrderLineItem`.

---

## Chapter 10 - Testing microservices: Part 2

**The book's idea.** Above unit tests in the pyramid: **consumer-driven contract tests** (the consumer of an API defines the shape of interaction it expects, and the producer is automatically verified against that contract - catching a breaking API change before it ever reaches a real integration), **component tests** (spin up one service's real internal wiring - controllers through to its database and message broker - without the cost of the entire multi-service system), and **end-to-end tests** (drive the real, full, deployed system exactly the way an actual client would, deliberately kept to very few scenarios because of how slow and brittle they are compared to everything below them).

**A concrete example.** A contract test between `ftgo-mobile-gateway` (consumer) and `ftgo-order-service` (producer) fails the build the moment order-service renames a field the gateway depends on - *before* anyone deploys anything, and without either service's real database or message broker running. A component test for order-service spins up order-service for real (with a real, disposable MySQL and Kafka via Testcontainers) but stubs out every *other* service, checking order-service's own wiring is correct in isolation. An end-to-end test does neither shortcut: it drives a full Create → Revise → Cancel Order journey through the real public gateway against the entire Docker Compose stack, exactly as a real client would experience it.

**What was actually built.** All three levels, covering the `Order` sagas: REST, pub/sub, and async request/response consumer-driven contracts via Spring Cloud Contract Verifier (including a hand-written embedded-Kafka bridge for the two Kafka-based contracts, since this project isn't on Spring Cloud Stream); an out-of-process Cucumber component test for order-service's Place Order flow with restaurant-service stubbed via WireMock and the other saga participants stood in by a single Kafka-based stub; and one Cucumber end-to-end scenario driving Create→Revise→Cancel Order through `ftgo-public-gateway` against the full stack. See `docs/ARCHITECTURE.md`'s "End-to-end testing" section.

---

## Chapter 11 - Developing production-ready services

**The book's idea.** A service isn't actually production-ready just because its business logic works. It also needs: a way for an **authorization server** to issue verifiable identity/permission tokens and for every service to check them; **externalized configuration** (config that lives outside the deployed artifact, so it can change without a rebuild); and observability - **health checks**, **application metrics**, **distributed tracing**, **log aggregation**, and **exception tracking** - plus **audit logging** (a record of who did what, succeeding or not). The chapter's real theme is that production-readiness is almost entirely *cross-cutting*: every one of these has to reach every service, so the interesting engineering question each time is *how* it gets applied everywhere consistently, not what it individually does.

**A concrete example.** Distributed tracing only becomes useful once a single request's `traceId` shows up consistently in every log line and every metric it touches across every service it passed through - a trace that stops at one service's boundary, or a log line with no `traceId` at all, can't be correlated with anything and might as well not exist. That correlation, not any single tool, is the actual payoff of this chapter.

**What was actually built.** All of it, delivered through three different cross-cutting mechanisms depending on what fit: shared library code auto-registered so no service opts in by hand (`ftgo-common`'s `AuditLoggingAspect`), a shared Gradle build block applying dependencies uniformly across services, and pure infrastructure that needs nothing from the service code at all (Filebeat scraping stdout logs, Prometheus scraping `/actuator/prometheus`). Concretely: a new `ftgo-authorization-server` issuing JWTs; `ftgo-config-server` for externalized, live-refreshable configuration; health checks and Prometheus metrics with a Grafana dashboard on all ten services; distributed tracing via OpenTelemetry to Grafana Tempo; an ELK log stack correlated to traces by `traceId`; GlitchTip for uncaught-exception tracking; and a new `ftgo-audit-log-service` recording every audited endpoint call. See `docs/ARCHITECTURE.md`'s Chapter 11 sections (health checks, metrics, tracing, log aggregation, exception tracking, externalized configuration, authentication & authorization, audit logging) for the full technical detail on each.

---

## Chapter 12 - Deploying microservices

**The book's idea.** Getting services running reliably in production is its own set of patterns: packaging each service as a **container** (self-contained, runs the same way everywhere), running a fleet of them with an orchestration platform like **Kubernetes**, and - once you have enough services talking to each other - a **service mesh**: a layer of per-pod network proxies (**sidecars**, since each one sits alongside its application container in the same pod) that transparently add encryption, retries, and traffic-shaping to every request, without the application code knowing.

**A concrete example.** Without a service mesh, if you want every service-to-service call encrypted, every service has to implement TLS itself. With a mesh (this project uses Linkerd), a sidecar proxy is injected into every pod, and it automatically encrypts (mTLS) and can retry every outbound call - the application code is unaware it's even happening.

**What was actually built.** Docker images and Docker Compose for the whole stack (the book's §12.1-12.3, see `docs/CH12-DEPLOYMENT.md`); the entire stack redeployed to a local Kubernetes cluster (`kind`) via a Helm chart, verified with the end-to-end test suite running against it; zero-downtime rolling deployments; and a Linkerd service mesh providing automatic mutual TLS between every pod. A genuinely useful negative finding came out of the mesh work: Linkerd's traffic-management features (retries via a `ServiceProfile`) only apply to traffic that Kubernetes' own Service naming actually carries - since `order-service` locates its dependencies through Eureka (a Ch.3 decision) rather than Kubernetes Service DNS, its calls resolve straight to a pod IP and the mesh never sees them as Service-addressed traffic, so the `ServiceProfile`s configured for it have no effect on its real traffic. This was verified empirically (identical behavior with the policy present or deleted) rather than assumed from the configuration alone - a useful reminder that deploying a policy without error isn't the same as it actually engaging. See `docs/ARCHITECTURE.md`'s "Kubernetes deployment" section for the full picture, including this finding.

---

## Chapter 13 - Refactoring to microservices

**The book's idea.** This chapter is about the opposite starting point from the rest of the book: you already have a working monolith, and want to migrate it to microservices *incrementally*, without a risky big-bang rewrite. This is the **strangler application** approach, via three levers: implement new features as standalone services (fast, low-risk, stops the monolith growing further); split the presentation tier from the backend (a partial win - two smaller monoliths, not real services yet, but exposes a remote API); and extract existing business capabilities out of the monolith one at a time (the lever that actually shrinks it, and the hard one). Extracted pieces and the remaining monolith keep talking via **integration glue**, always behind an **anti-corruption layer** - a translation boundary that stops the monolith's old, often messy domain model from leaking into the new service's clean one.

**A concrete example.** The book's own worked example is extracting a `Delivery` entity out of an `Order` god-class inside the monolith - a single class doing too much (order details *and* delivery tracking *and* everything else) gets split into two, with the new Delivery Service reached from the monolith through an anti-corruption layer rather than the monolith reaching directly into the new service's internals.

**What was actually built.** Nothing in code - this project was built as microservices from Chapter 1 onward, so there's no monolith here to strangle. The chapter was read in full for its concepts rather than skipped, because two of its ideas are genuinely useful even without a monolith to migrate: the insight that *the order you extract services in* determines whether the remaining monolith needs compensating-transaction logic added to it at all (extracting in the same order this project's own Chapter 4 saga already uses - Order, then Consumer, then Kitchen, then Accounting - lets the monolith's own step always be the saga's point of no return, needing no rollback logic); and the dual-mode auth trick (a monolith adds one extra JWT cookie alongside its normal session cookie, and the gateway translates that cookie into an `Authorization` header for calls to extracted services) - conceptually the same problem this project's own `ftgo-authorization-server` (Chapter 11) solved by starting fresh with JWTs rather than needing to bridge from a legacy mechanism.

---

## Interview prep cheat sheet

Every pattern above is a common system-design interview topic. This section pairs the question you're likely to be asked with a short answer *and* this project's own concrete example - so instead of a memorized definition, you have a real system to point to ("here's how I actually built that").

**"How would you decide where to draw service boundaries?"**
Decompose by business capability and by DDD subdomain; use bounded contexts, since the same real-world noun (an "order") can mean different things in different parts of the system.
Example: this project's Order/Ticket/Delivery/Authorization split - one real-world order, four different bounded-context models.

**"How do you keep data consistent across services without distributed transactions?"**
Sagas: a sequence of local transactions, each with a compensating action if a later step fails. Two coordination styles - choreography (event-reactive, no coordinator) and orchestration (a central orchestrator issues each step).
Example: the Create Order saga, implemented both ways and switchable via `SAGA_MODE`, reaching identical outcomes either way.

**"What's the difference between choreography and orchestration sagas, and when would you pick one over the other?"**
Choreography has no single point of control or failure but makes the overall flow hard to see in one place; orchestration centralizes the flow (easy to reason about, easy to add steps) but the orchestrator becomes a new component to build and depend on.
Example: both exist side by side for all three `Order` sagas here specifically to compare them, not because one is "correct."

**"How do you reliably publish an event when you save something to a database?"**
The transactional outbox pattern: write the event as a row in the same database transaction as the state change, then a separate poller publishes it - avoiding the "saved but never published" / "published but the save rolled back" failure modes of trying to write to two different systems at once.
Example: `order-service` writing `OrderCreated` to its outbox table in the same transaction as the `Order` row.

**"What's a circuit breaker and why do you need one?"**
It stops calling a dependency that's clearly failing, so callers fail fast instead of piling up slow timeouts and going down together with the dependency they called.
Example: `order-service`'s Resilience4j-wrapped call to `restaurant-service`, with sliding-window failure detection and a wait period before trying again.

**"API composition vs. CQRS - what's the actual trade-off?"**
API composition reads live, composed data at request time - always current, but as available and as fast as the slowest dependency called. CQRS maintains a pre-built read model kept up to date by consuming events - instant and fully decoupled reads, but eventually (not immediately) consistent.
Example: `GET /orders/{id}/view` (API composition, parallel fan-out with per-section degradation) vs. `ftgo-order-history-service`'s `GET /order-views/{orderId}` (CQRS, Kafka-driven read model), both implemented so the trade-off could be felt directly.

**"What is event sourcing, and how is it different from just storing current state?"**
Store every state-changing event forever, in order, and derive current state by replaying them - instead of overwriting a row with the latest values. The discipline that makes it work: separate the step that *decides* what happened from the step that *applies* an event that already happened, since the second one has to run identically for both a brand-new event and every historical one during a replay.
Example: `Order`'s hand-rolled event store, with a dedicated optimistic-lock version table separate from the event count, and snapshots purely as a performance optimization with no effect on correctness.

**"What's a DDD aggregate, and what are the rules around it?"**
A cluster of objects treated as one consistency boundary, modified together through one aggregate root. Rules: reference only the root from outside; reference other aggregates by primary key, never by object reference; one transaction touches exactly one aggregate (which is exactly why sagas exist - a business operation spanning multiple aggregates can't be one local transaction).
Example: `Order` referencing a consumer only by `consumerId`, never by holding an actual `Consumer` object reference.

**"How do you test a system built this way, given you can't practically spin up everything for every test?"**
The test pyramid: lots of cheap unit tests at the base (sociable for entities/aggregates/sagas, solitary - mocked collaborators - for services/controllers/handlers), consumer-driven contract tests to catch cross-service breaking changes without running both services, component tests for one service's real internal wiring against disposable infra, and very few full end-to-end tests for the handful of journeys that matter most.
Example: a Pact-based contract between `ftgo-mobile-gateway` and `ftgo-order-service` catching a renamed field before deployment, and a single Cucumber end-to-end scenario driving Create→Revise→Cancel Order through the real gateway and full stack.

**"What makes a service actually production-ready, beyond the business logic working?"**
Security (issued, verifiable tokens), externalized configuration (changeable without a rebuild), and observability - health checks, metrics, distributed tracing, log aggregation, exception tracking - plus audit logging. The hard part is making all of that reach every service *consistently*, not implementing any one piece.
Example: `traceId` correlating a Kibana log line to a Tempo trace span across every service a request passed through - the correlation, not any individual tool, is what actually pays off.

**"What does a service mesh give you that client-side service discovery (like Eureka) doesn't, and can you use both together?"**
A mesh adds transparent, per-pod features (mTLS, retries, traffic shaping) without application code changes, but its traffic-management features are keyed on Kubernetes Service identity - if your application resolves calls itself (e.g. via Eureka straight to a pod IP), the mesh never sees that traffic as Service-addressed, and its policies silently don't apply to it.
Example: this project's own empirical finding - Linkerd `ServiceProfile` retries configured for `order-service`'s downstream calls had zero effect, verified by identical fault behavior whether the policy was present or deleted, because `order-service` still resolves those calls via Eureka.

**"How would you migrate an existing monolith to microservices without a risky rewrite?"**
The strangler application approach: implement new features as standalone services first (fastest, lowest risk), then extract existing capabilities from the monolith one at a time, always behind an anti-corruption layer so the monolith's messier domain model doesn't leak into the new services. The order you extract in matters: extracting in an order where the monolith's own step is always a saga's "point of no return" means the monolith never needs compensating-transaction logic added to it.
Example: this project has no monolith, but the underlying insight - saga step order as a lever for minimizing risky changes to the piece you haven't migrated yet - is a genuinely transferable idea from Chapter 13.

---

## Where to go next

- **`docs/ARCHITECTURE.md`** - the technical reference: sequence diagrams for every saga (both styles), the Kafka topic catalog, and a deep dive into every pattern summarized above.
- **`docs/CH12-DEPLOYMENT.md`** - the container/Compose deployment details for Chapter 12's first sub-project.
- **`README.md`** - what's running today: the full service list, ports, and how to bring the stack up locally.
- **`CONTEXT.md`** - a working learner's notebook: session-by-session log of what was built when, plus a running "Understood well" / "Needs more depth" account of the book's concepts.
