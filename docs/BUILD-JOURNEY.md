# The FTGO Build Journey

A detailed, chapter-by-chapter walkthrough of how this project was built, following the book *Microservices Patterns* by Chris Richardson (the "FTGO" - Food To Go - example application the book itself is built around) - written to be **self-sufficient staff engineer interview preparation** on microservices architecture and system design.

## What this document is for

`README.md` tells you what the system *is* today.
`docs/ARCHITECTURE.md` is a technical reference - diagrams, sequence flows, exact class names.
This document is different: it's a **teaching narrative plus an interview-prep guide**, chapter by chapter, through both the book and this codebase.

Every chapter section below has the same five parts:

1. **The book's idea** - the pattern, in plain English, including the forces/trade-offs that make it non-obvious (not just a definition).
2. **Alternatives and why they lose** - what else you could do instead, and specifically why the book's pattern usually wins (or when it doesn't).
3. **A concrete example** - this project's own implementation of the idea, or the book's canonical example where this project didn't need to build that specific facet.
4. **What was actually built** - the real modules/services/classes, with links to the deeper technical reference (`docs/ARCHITECTURE.md`) and the pitfalls hit along the way.
5. **Interview prep** - a chapter-specific set of questions, ranging from "explain the pattern" (screening-level) through "what would you change at 100x scale" and "walk me through a trade-off you'd defend to a skeptical staff+ panel" (staff-level). Each answer references this project's real example, so you're not reciting a definition - you're describing something you actually reasoned through and built.

Read it top to bottom like a book, or jump straight to any chapter's Interview prep section if you're cramming.

A few acronyms recur constantly, so here they are once, up front:

- **DDD** - Domain-Driven Design: designing your code's structure around the real business concepts (Order, Ticket, Delivery) rather than around technical layers.
- **CQRS** - Command Query Responsibility Segregation: using a different model for writing data than for reading it.
- **BFF** - Backend for Frontend: a small API gateway built for one specific kind of client (e.g. the mobile app).
- **CDC** - Change Data Capture: watching a database's transaction log to notice changes, instead of the application explicitly announcing them.
- **mTLS** - mutual TLS: both sides of a connection prove their identity with a certificate, not just the server.
- **JWT** - JSON Web Token: a signed, tamper-proof blob of claims (like "this is user 42, role CONSUMER") that a server can trust without a database lookup.
- **SLA/SLO** - Service Level Agreement/Objective: a promise (or internal target) about a system's availability or latency.
- **Idempotency** - performing an operation twice has the same effect as performing it once; critical for safe retries.

---

## What is FTGO?

FTGO is a food delivery application - think "order food from a restaurant, a courier picks it up and delivers it." It's the book's running example precisely because it's simple to describe but has enough moving parts (placing an order, paying for it, preparing it, delivering it) to need *real* microservices patterns, not toy ones. This project builds FTGO from scratch, chapter by chapter, as a way to learn those patterns by doing rather than just reading, and this document captures both the learning and a reusable interview-prep reference.

---

## Chapter 1 - Escaping monolithic hell

### The book's idea

A single, growing codebase ("a monolith") eventually becomes hard to work with: every change risks breaking something unrelated, the whole thing has to be redeployed for a one-line fix, a bug in one module can take the whole process down, and different parts of the system that want different technology stacks or scaling profiles are stuck sharing one. The book calls this "monolithic hell" and names its specific symptoms: complexity that overwhelms individual developers, slow development, a difficult path from commit to deployment, difficulty scaling, a struggle to deliver reliable applications, and being locked into an increasingly obsolete technology stack.

The book frames microservices through the **scale cube**, three independent axes along which you can scale a system:

- **X-axis**: run multiple identical copies behind a load balancer (horizontal duplication). Cheap, but doesn't reduce per-request complexity or database contention.
- **Y-axis**: split the application by *function* into separate services. This is what microservices actually is.
- **Z-axis**: partition data (sharding) so each instance handles a subset of the data (e.g. by customer ID range).

Microservices is specifically a Y-axis decomposition - and the book is explicit that X and Z axis scaling still apply *within* each service, they're not alternatives to Y-axis, they're complementary.

### Alternatives and why they lose

- **Just scale the monolith horizontally (X-axis only).** Works for stateless CPU/throughput scaling, but doesn't fix the *organizational* problem - hundreds of engineers still committing to one codebase, one build, one deploy pipeline, one blast radius. The book's core argument is that monolithic hell is mostly a *coordination* and *blast-radius* problem, not a raw-scaling problem, so scaling alone doesn't fix it.
- **A "modular monolith"** (strict internal module boundaries, single deployable). Genuinely useful for smaller teams/lower complexity - the book doesn't claim microservices is always correct. It's a real, valid middle ground, but it doesn't get you independent deployability, independent scaling, or technology heterogeneity, which are the specific things microservices trades complexity for.

### A concrete example

Imagine one giant `FoodDeliveryApplication` where the code for taking orders, the code for tracking restaurant menus, and the code for paying couriers all live in the same process and share one database (and often one giant table) for "everything." A bug in the payment code can't be fixed without redeploying (and re-testing) the entire order-taking flow too, and a memory leak in the menu-browsing code can crash the process that's also mid-way through charging a customer. Splitting that into an Order Service, a Restaurant Service, and an Accounting Service - each with its own database, deployed independently - means a payment bug fix touches only the Accounting Service, and a memory leak there can't take down order placement.

### What was actually built

This chapter is conceptual: there's no monolith in this codebase to escape, because the project was built as microservices from day one. The chapter was read and its concepts - the scale cube, and *why* microservices is specifically a Y-axis split by function - are the mental model behind every later architectural decision, even though no code came out of this chapter directly.

### Interview prep

**Q: When would you *not* recommend microservices to a team?**
When the team is small (a handful of engineers), the domain isn't well understood yet, or there's no existing operational maturity (CI/CD, monitoring, on-call) to run many independently deployed services. The book itself frames microservices as a trade - you're buying independent deployability, scalability, and fault isolation, and paying for it with distributed-systems complexity (network calls that can fail, eventual consistency, harder end-to-end debugging). A modular monolith with strict internal boundaries can capture much of the organizational benefit at a fraction of the operational cost, and can be split later once boundaries have proven themselves in production.

**Q: Explain the scale cube and place this project's own scaling choices on it.**
X-axis is running more identical instances (this project's services can each be scaled horizontally in Kubernetes, per `docs/ARCHITECTURE.md`'s Kubernetes deployment section); Y-axis is splitting by function, which is the whole point of the Order/Kitchen/Accounting/Delivery/Restaurant service split from Chapter 2 onward; Z-axis (sharding by data partition) wasn't needed at this project's scale, but each service's own database could, in principle, be sharded independently without affecting any other service - which is itself a benefit of the Y-axis split: Z-axis scaling decisions become local to one service instead of a whole-system migration.

**Q: What's the actual cost of the "distributed monolith" anti-pattern, and how do you know you've built one?**
A distributed monolith is microservices in deployment topology only - services are still tightly coupled (shared database, synchronous call chains that must all succeed together, coordinated releases across services). You've built one if you can't deploy one service without also deploying others, or if one service's outage reliably takes others down with it. It has *all* the operational cost of microservices (network calls, more moving parts, more to monitor) and none of the benefit (independent deployability, fault isolation) - the worst of both worlds. This project deliberately avoids it: every service owns its own database (Chapter 2-3), failures are isolated with circuit breakers (Chapter 3), and cross-service consistency uses sagas instead of distributed transactions (Chapter 4) specifically so no two services have to deploy or fail together.

**Q: A staff-level panel asks you to justify a microservices migration to a skeptical VP who only sees added complexity. What's your answer?**
Reframe the question: the complexity doesn't disappear in a monolith, it's just hidden as *implicit coupling* - and implicit coupling is worse because it's invisible until it causes an incident. Microservices makes coupling explicit (a network call you can see, log, and circuit-break) in exchange for genuine independent deployability and fault isolation. The honest answer to a skeptical VP is that this trade is worth it specifically when release velocity or reliability is already suffering from the monolith's coordination cost - it's not worth it as a default, and a staff engineer's job is to make that call explicitly with data (deploy frequency, incident blast radius, team size) rather than following a trend.

---

## Chapter 2 - Decomposition strategies

### The book's idea

If microservices means "split the system," the hard question is *where* to draw the lines - get this wrong and you end up with a distributed monolith (Chapter 1) or services so fine-grained that every operation is a coordination nightmare. The book gives complementary techniques:

- **Decompose by business capability**: what does the business *do* - "manage orders," "manage deliveries," "manage payments" - derived from the org's own structure and purpose, largely stable even as implementation details change.
- **Decompose by DDD subdomain**: what are the distinct areas of knowledge and vocabulary, independent of current org structure - useful specifically when the org structure itself is dysfunctional or not yet settled, since it derives boundaries from the *problem domain* rather than from whoever currently owns what.
- **Bounded context**: a boundary inside which a term means one specific, unambiguous thing - because the same real-world word means different things to different parts of the business, and pretending otherwise produces a bloated, contradictory shared model.

The book also gives decomposition guidelines grounded in classic OO design principles applied at the service level: the **Single Responsibility Principle** (a service should have one reason to change) and the **Common Closure Principle** (things that change together should live together) - both arguments for cohesion within a service and loose coupling between services.

### Alternatives and why they lose

- **Decompose by technical layer** (a "presentation service," a "business logic service," a "data access service"). Seems clean on a diagram but is almost always wrong: a single business change (e.g. "add a delivery instructions field") now requires coordinated changes and deployments across all three layer-services, which is exactly the coordination cost microservices is supposed to remove.
- **Decompose by team/org chart alone, ignoring the domain.** Reasonable as a starting heuristic (Conway's Law says your system will end up mirroring your org structure anyway), but dangerous if the org structure itself is currently wrong for the domain - you'll bake a bad boundary into your architecture and then need an org change *and* an architecture change to fix it later.

### A concrete example

The word "Order" means different things in different places: to the kitchen it's a **Ticket** (what to cook, in what sequence, when it's ready); to the courier it's a **Delivery** (where to pick up, where to drop off, current location); to accounting it's an **Authorization** (how much to charge, whether the charge succeeded). Each of those is its own bounded context with its own model and its own vocabulary, even though a person outside the system would call all of them "the order." Trying to force one shared `Order` class to serve all three concerns is exactly the kind of god-class problem this chapter's decomposition techniques are meant to prevent.

### What was actually built

Again conceptual - no code - but this is the chapter that explains *why* this project ended up with separate `ftgo-order-service`, `ftgo-kitchen-service`, `ftgo-delivery-service`, and `ftgo-accounting-service` modules instead of one big "orders" module: each one is a different bounded context, matching the book's own Ticket/Delivery/Authorization split almost exactly. This decision shapes every later chapter - it's the reason Chapter 4's sagas are needed at all (an operation spanning bounded contexts can't be one local transaction) and the reason Chapter 5's aggregate rules matter (each bounded context gets to define its own model without a neighboring context's concerns leaking in).

### Interview prep

**Q: Walk me through how you'd decompose a system you've never seen before into services.**
Start from business capabilities, not existing code structure - list what the business *does* (place an order, manage a menu, process a payment, arrange delivery), not what modules currently exist. Cross-check with DDD subdomain analysis and specifically look for the same noun meaning different things to different capabilities (a strong signal of a bounded-context boundary, like this project's Order/Ticket/Delivery/Authorization split). Validate candidate boundaries against the Single Responsibility and Common Closure Principles - if two "services" always change together, they're probably one service; if one "service" changes for unrelated reasons, it's probably two.

**Q: What's a bounded context, and why does ignoring it cause real production pain?**
A boundary inside which a specific term has one unambiguous meaning. Ignoring it produces a single shared model (often literally one shared "Order" class or table) that every team bolts fields onto for their own purposes - it grows without limit, no one fully understands it, and a change for one team's purpose routinely breaks another team's unrelated usage of the same field. This project sidesteps that entirely by giving Kitchen, Delivery, and Accounting their own models (Ticket, Delivery, Authorization) instead of one shared Order god-object, each independently free to evolve.

**Q: How do you handle a decomposition disagreement between two teams who both think they should own a given capability?**
This is fundamentally a Conway's Law question as much as a technical one - the org structure and the architecture need to agree, or one of them will silently lose. Push for a decision grounded in the domain model, not politics: which team's bounded context does the disputed data/behavior actually belong to, based on who defines its ubiquitous language? As staff engineer, the useful move is often making the *cost* of the wrong answer visible (a shared "everyone touches this" model becomes exactly the god-class problem this chapter warns about) rather than trying to win the argument on authority.

**Q: You decomposed a system, and now you realize two services always deploy together and constantly call each other synchronously in a tight request/response loop. What does that tell you, and what do you do?**
That's a strong signal the boundary is wrong - probably one bounded context was split into two services rather than genuinely being two capabilities. The fix is usually to merge them back (or at minimum, seriously reconsider the boundary) rather than trying to paper over tight coupling with more infrastructure (retries, circuit breakers, caching). A staff engineer should recognize this pattern early - excessive synchronous chatter between two services is a decomposition smell, not something to solve by adding more resilience patterns on top.

---

## Chapter 3 - Interprocess communication

### The book's idea

Once services are separate processes, they need to talk to each other, and there's more than one way to do it, each with real trade-offs:

- **Synchronous request/response** (e.g. REST, gRPC): simple mental model, but the caller is now coupled to the callee's *availability* - if the callee is down or slow, the caller is affected too, and chains of synchronous calls compound this (a call across 5 services, each 99.9% available, is only ~99.5% available end to end).
- **Asynchronous messaging**: a service publishes an event or command and moves on without waiting; the receiver processes it whenever it can. Decouples availability (the publisher doesn't need the consumer to be up right now) at the cost of a harder mental model (eventual consistency, message ordering, at-least-once delivery meaning consumers must handle duplicates).
- **Circuit breaker**: stop calling a dependency that's clearly failing, instead of piling up slow timeouts that can exhaust the caller's own thread pool and take *it* down too - a specific defense against cascading failure.
- **Service discovery**: given that service instances come and go (scaling, deploys, crashes), how does a caller find *where* a healthy instance currently is? Client-side discovery (the caller queries a registry itself, e.g. Eureka) vs. server-side discovery (a load balancer/proxy does it transparently, e.g. Kubernetes Services, or a service mesh).
- **Transactional outbox**: how do you reliably do "save to my database" *and* "publish an event about it" as a single atomic unit, when the database and the message broker are two separate systems that can't be updated in one transaction?

### Alternatives and why they lose

- **Two-phase commit across a database and a message broker.** Theoretically atomic, practically almost never used in this space - it requires broker support most popular brokers (Kafka included) don't provide, and even where supported, holds locks across a network round-trip, badly hurting throughput and availability. The outbox pattern gets effectively the same guarantee (publish happens if and only if the DB commit happens) using only the database's own local transaction.
- **Publish-then-save, or save-then-publish, with no outbox.** Either order has a failure window: publish-then-save can announce something that then fails to save; save-then-publish can save something and then crash before publishing, silently losing the event. Both are real, observed classes of bugs, not theoretical - this is exactly why the outbox pattern exists.
- **Skip the circuit breaker, just add a timeout.** A timeout alone still lets every caller thread block for the full timeout duration on every call to a failing dependency - if calls arrive faster than the timeout, the caller's own thread pool exhausts and it goes down too. A circuit breaker adds fail-fast behavior on top of a timeout specifically to prevent this cascade.

### A concrete example

`ftgo-order-service` needs restaurant details to build an order. A naive synchronous call (`GET /restaurants/{id}`) works until restaurant-service is slow or down - then every order attempt hangs waiting on it too, and if enough hang at once, order-service's own thread pool fills up and *it* stops responding to anything, including requests that had nothing to do with restaurant-service. Wrapping that call in a circuit breaker means: after enough recent failures, stop calling restaurant-service for a while and fail fast instead, so order-service stays responsive even when its dependency isn't.

Separately, when order-service creates an Order, it also needs to tell the rest of the system "a new order was created" - but if it writes the Order row to its database and *then* tries to publish to Kafka, a crash between those two steps means a saved order nobody else ever hears about (or, if published first, an event about an order that was never actually saved). The transactional outbox fixes this by writing the "event to publish" as a row in the *same database transaction* as the Order itself, so both succeed together or neither does; a background poller then reads that table and actually publishes to Kafka, at-least-once.

### What was actually built

- **Synchronous REST + circuit breaker**: `order-service` calls `restaurant-service`'s `GET /restaurants/{id}` through a Resilience4j circuit breaker (2s connect/read timeout, sliding-window-size 5, failure-rate-threshold 50%, 5s wait-duration-in-open-state). Verified: happy path (201/APPROVED), circuit opening under sustained failure (first 5 calls ~2s each, then fail-fast in ~15-20ms), and recovery to CLOSED after the wait duration.
- **Asynchronous messaging + transactional outbox**: `order-service` writes an `OrderCreated` event to an outbox table in the same transaction as the Order row; a scheduled poller publishes it to the `order.events` Kafka topic, which `kitchen-service` consumes.
- **Service discovery**: a standalone Eureka server (`ftgo-service-registry`), used by every service to find each other by name instead of hardcoded addresses - a decision whose long-tail consequence surfaces much later, in Chapter 12 (a service mesh's Kubernetes-Service-based traffic policies turn out to have no effect on traffic that Eureka already resolved to a raw pod IP).
- **Transaction log tailing (CDC)**: explored as an alternative outbox-publishing mechanism (reading the database's own change log instead of polling a table) - lower latency and no polling overhead, at the cost of needing CDC infrastructure (e.g. Debezium) tied to the specific database engine.

See `docs/ARCHITECTURE.md`'s "The transactional outbox pattern" and "Kafka topic catalog" sections for the full technical picture.

### Interview prep

**Q: Explain the transactional outbox pattern, and why a naive "save then publish" is broken.**
"Save then publish" has a failure window between the two operations: the process can crash (or the publish can simply fail) after the database commit but before the message is sent, silently losing an event that other services depend on. The outbox pattern makes publishing part of the same local database transaction as the business write: instead of calling the broker directly, you insert a row representing the event into an "outbox" table, in the same transaction as the Order row. A separate poller (or CDC process) reads unsent outbox rows and publishes them, retrying until it succeeds - so the event is *guaranteed* to eventually be published if and only if the business write committed. The trade-off: consumers now see at-least-once delivery (the poller can publish the same row twice if it crashes between publishing and marking it sent), so every consumer must be idempotent.

**Q: Polling an outbox table vs. transaction log tailing (CDC) - what's the actual trade-off?**
Polling is simpler to implement and portable across databases, but adds latency (bounded by the poll interval) and continuous read load on the outbox table even when there's nothing new. CDC (reading the database's write-ahead/binlog directly, e.g. via Debezium) gets near-real-time publishing with near-zero polling overhead, but ties you to database-specific tooling and adds an extra piece of infrastructure to run and operate. At small-to-medium scale the polling approach's simplicity usually wins; at high event volume or when latency matters, CDC's efficiency wins.

**Q: How do you size a circuit breaker's parameters (failure threshold, window size, wait duration) in production, and what happens if you get them wrong?**
Too sensitive (small window, low threshold) and normal, brief blips trip the breaker unnecessarily, causing needless fail-fast responses during transient noise. Too lax (large window, high threshold) and the breaker doesn't open until the caller has already suffered a large number of slow failures, defeating the point. In practice you size these from observed latency/error-rate distributions under real load (p99 latency, baseline error rate) rather than guessing, and the wait-duration-in-open-state should be long enough that a real recovery is likely, but short enough that you're not needlessly rejecting traffic once the dependency is actually healthy again - this project used a 5s wait after tuning against observed recovery time in testing, not a default value picked blind.

**Q: A service call chain is 6 services deep, each individually 99.9% available. What's the actual end-to-end availability, and how would you fix it if it's not good enough?**
Roughly 0.999^6 ≈ 99.4% if every hop is purely synchronous and required - noticeably worse than any individual service, because failures compound multiplicatively across the chain. Fixes: replace synchronous chains with asynchronous messaging wherever the caller doesn't actually need an immediate answer (removes that hop from the availability chain entirely); make individual calls optional/degradable rather than required (this project's `SectionResult`-style graceful degradation in Chapter 7's API composition is exactly this - a failed downstream section degrades only its own part of the response, not the whole request); and reduce the chain depth by reconsidering whether all 6 hops are genuinely necessary, which is sometimes itself a decomposition smell (Chapter 2).

**Q: What's the difference between client-side and server-side service discovery, and which did this project pick, and why does that choice matter later?**
Client-side discovery (this project's choice, via Eureka): the calling service itself queries a registry and picks an instance, giving it full control (custom load-balancing logic) at the cost of needing a discovery-aware client library in every language/service. Server-side discovery (e.g. a Kubernetes Service, or a service mesh's sidecar proxy): a separate component does the resolution and routing transparently, so application code doesn't need to know about discovery at all - simpler application code, but less control, and it requires the platform underneath to support it. This choice matters far beyond Chapter 3: in Chapter 12, deploying a service mesh (Linkerd) revealed that its traffic-management features only govern traffic addressed to a Kubernetes Service - since this project's services resolve each other via Eureka straight to pod IPs, the mesh's retry policies had zero effect on that traffic, discovered only through live fault-injection testing, not by reading the mesh's configuration.

---

## Chapter 4 - Managing transactions with sagas

### The book's idea

Placing an order touches multiple services (reserve credit, notify the kitchen, arrange delivery) - each with its own database, by design (Chapter 2/3's decomposition). A traditional multi-service ACID transaction (lock everything involved, commit or roll back together, via something like two-phase commit) doesn't scale across independently owned databases: it requires holding locks across a network round-trip through every participant, badly hurting throughput and availability, and most NoSQL stores and message brokers don't even support it.

A **saga** is the alternative: break the operation into a sequence of local transactions, each in one service, where every step that can fail has a matching **compensating transaction** to semantically undo the effects of the steps that already succeeded (you can't literally roll back a committed local transaction in another service - you run a new transaction that logically reverses it). Sagas trade strict ACID isolation for eventual consistency, plus two extra concerns the book names explicitly: **countermeasures for lack of isolation** (since a saga's intermediate states are visible to other transactions before the saga finishes, unlike an ACID transaction) - e.g. a semantic lock (marking a record as "pending" so other transactions know not to touch it yet), and correctly ordering steps so any step that can still fail happens *before* the "point of no return" (the step after which nothing can be compensated, e.g. actually charging a card or actually cooking food).

Sagas can be coordinated two ways:

- **Choreography**: each service reacts to events published by the previous one - no central coordinator, each service only needs to know "what event do I react to and what do I do/publish next." Fully decentralized, but the overall flow isn't visible in any one place, and adding a new step means every participant in the chain that touches it needs to know about the new event.
- **Orchestration**: a dedicated saga orchestrator explicitly tells each participant what to do next, step by step, and reacts to each reply. The flow is visible and easy to reason about in one place, but the orchestrator is now a new component with its own logic, its own potential bugs, and (if built naively) a new single point of coordination.

### Alternatives and why they lose

- **Distributed (2PC) transactions.** As above - most brokers/NoSQL stores don't support the required "prepare" protocol, and even where technically possible, it holds locks across the network for the full duration of the slowest participant, which is a severe availability and throughput cost microservices architectures specifically try to avoid.
- **Just accept some inconsistency and reconcile later with a batch job.** Sometimes genuinely reasonable for low-stakes data, but for something like "was the customer actually charged for an order that was never created," an unbounded inconsistency window is usually unacceptable - sagas give you a bounded, well-defined sequence with explicit compensation instead of an ad hoc nightly cleanup script.

### A concrete example

Placing an order: 1) order-service creates an Order in `PENDING` state, 2) accounting-service reserves credit, 3) kitchen-service creates a Ticket, 4) order-service marks the Order `APPROVED`. If step 2 fails (insufficient credit), there's no step 3 or 4 to run - the saga instead runs a compensating action: order-service marks the Order `REJECTED`. Nothing was ever locked across services; each step was its own local, committed transaction, and failure was handled by *moving forward* with a corrective action, not by rolling back time. Note the ordering choice: credit reservation (step 2, cheaply reversible - just release the reservation) happens *before* Ticket creation (step 3, a real-world action with a human cost to undo) - a direct application of the book's "compensatable steps before the pivot" ordering principle.

### What was actually built

The **Create Order saga**, implemented *both* ways so the two coordination styles could be compared directly rather than just read about, switchable at runtime via a `SAGA_MODE` environment variable. In choreography mode, each service publishes a domain event and the next participant reacts to it; in orchestration mode, a `CreateOrderSagaOrchestrator` in order-service explicitly issues each command and reacts to each reply. Both reach the identical outcome for the same inputs - verified directly, not assumed. See `docs/ARCHITECTURE.md`'s "Create Order saga - choreography" and "- orchestration" sections for the full step-by-step sequence diagrams.

### Interview prep

**Q: Explain sagas, and specifically why "just use 2PC" isn't the answer.**
A saga replaces one distributed ACID transaction with a sequence of local transactions, each committed independently, with a compensating transaction defined for every step that could need undoing. 2PC requires every participant to support a "prepare" phase and hold locks until a coordinator says commit or abort - most message brokers and many NoSQL databases don't support this, and even when they do, holding cross-service locks for the duration of the slowest participant severely limits throughput and availability, which defeats much of the point of splitting into independently scalable services in the first place. Sagas trade strict atomicity and isolation for eventual consistency plus explicit compensation logic, which is a better fit for how microservices are actually deployed and scaled.

**Q: Choreography vs. orchestration - when would you pick each, and what breaks as you add more steps?**
Choreography has no single point of control (each service only knows its own reaction), which is elegant for a small number of steps, but as the saga grows, understanding the *overall* flow requires mentally tracing events across every participant's code - there's no one place to look. It also means adding a new step to the middle of the flow can require touching multiple existing services (whoever needs to react to the new event). Orchestration centralizes the flow in one orchestrator, which is easy to read top-to-bottom and easy to extend (add a step in one place), at the cost of a new component that itself needs testing, versioning, and failure handling (what happens if the orchestrator crashes mid-saga?). In practice, orchestration tends to win as saga complexity grows past a handful of steps - this project implements the Create Order saga both ways specifically to be able to point to this trade-off from direct experience rather than a textbook description.

**Q: What's a compensating transaction, and can you give an example where "just delete the row" isn't a valid compensation?**
A compensating transaction is a new, forward-moving local transaction that semantically reverses a previously committed step - not a literal undo/rollback, because the original transaction already committed and may have had externally visible effects. "Just delete the row" fails as a compensation whenever the original step had a real-world side effect beyond the database: if step 3 already notified the kitchen to start cooking, deleting the Ticket row doesn't un-cook the food - the actual compensation has to be something like "cancel the ticket and notify the kitchen to stop," a business-meaningful action, not a data-cleanup action. This is exactly why saga step *ordering* matters (see the next question) - you want the step that's genuinely hard/costly to compensate to happen as late as possible.

**Q: What's the "pivot transaction" in a saga, and why does step ordering matter so much?**
The pivot transaction is the step after which the saga can no longer be aborted - every step before it must be compensatable, and every step after it is assumed to eventually succeed (retried until it does, not compensated). Ordering steps so genuinely irreversible or costly-to-reverse actions (charging a card, starting food preparation, dispatching a courier) happen as late as possible - ideally right at or after the pivot - minimizes the blast radius of any single step failing, since fewer steps need real compensation logic. This project's Create Order saga puts credit reservation (cheaply reversible - release the hold) before Ticket creation (a real kitchen action) for exactly this reason.

**Q: How do you handle the lack of isolation between concurrent sagas touching related data - e.g. two orders for the last item in inventory, racing?**
The book calls this a "countermeasure for lack of isolation," and the most common concrete technique is a **semantic lock**: mark the record as "pending"/"reserved" as part of the first step, so a concurrent saga sees that state and can react appropriately (wait, reject, or queue) rather than proceeding as if the resource is still fully available. This is a deliberate, explicit design decision at the domain level - not something a database transaction gives you for free anymore, since the whole point of a saga is that no single transaction spans the whole operation.

**Q: You're asked to add a new step to an existing choreography saga in production, without downtime. What's the actual risk, and how do you mitigate it?**
The risk is a race during rollout: if the new step's participant is deployed before the services that need to react to its new event are ready (or vice versa), some in-flight sagas can either skip the new step or get stuck waiting for a reaction that isn't deployed yet. Mitigation: make the new step's event genuinely optional to consumers until they're ready (consumers that don't know about it simply ignore it, rather than blocking on it), roll out consumers before producers wherever possible, and treat the change like any other API/event schema evolution - backward compatible first, cut over second. This is also a strong argument, at staff-engineer scale, for orchestration over choreography for sagas that are still actively evolving - a new step in one orchestrator method is a much smaller, more contained change to reason about and roll out safely than a new event rippling through several independently deployed choreography participants.

---

## Chapter 5 - Designing business logic

### The book's idea

Where should the rules that govern business behavior actually live? The book contrasts:

- **Transaction script**: a service class with one procedural method per operation, manipulating data directly (often via SQL or an anemic data-access object), with no real object model behind it. Simple for genuinely simple logic, but tends to duplicate rules across methods and makes invariants (business rules that must always hold) hard to enforce consistently, since nothing stops one method from mutating state in a way another method assumed couldn't happen.
- **Domain model**: behavior lives on the entities themselves, so an invariant can be enforced in exactly one place (the entity's own methods) regardless of how many code paths might otherwise try to mutate it.

**DDD (Domain-Driven Design)** is a refinement of the domain model approach, organized around **aggregates**: a cluster of objects (like an Order and its line items) treated as one consistency boundary, always modified together, with one designated **aggregate root** that's the only object outside code is allowed to reference directly. Three rules keep aggregates well-behaved, and each one has a direct architectural consequence elsewhere in the book:

1. **Reference only the aggregate root from outside** - internal objects (like an individual `OrderLineItem`) are never referenced or mutated directly by other aggregates; all access goes through the root, so the root can always enforce its own invariants.
2. **Inter-aggregate references use primary keys, not object references** - `Order` holds `consumerId` (a plain ID), never an actual `Consumer` object reference. This keeps aggregates independently loadable/persistable and is what makes it possible for aggregates to eventually live in entirely separate services/databases.
3. **One transaction creates or updates exactly one aggregate** - this is the formal justification for *why sagas (Chapter 4) exist at all*: an operation that needs to touch multiple aggregates atomically simply isn't allowed to be one transaction, so it has to be a saga instead.

An **aggregate granularity** trade-off follows directly from rule 3: fine-grained aggregates (smaller, more focused) give better concurrency (fewer conflicting writes to the same aggregate) but need more sagas to coordinate across them; coarse-grained aggregates need fewer sagas but suffer more lock contention on the bigger aggregate. The book recommends fine-grained by default.

Domain events are the book's preferred way for an aggregate to announce "something happened": an aggregate method *returns* a `List<DomainEvent>` rather than the aggregate extending some `AbstractAggregateRoot`-style base class - keeping the aggregate itself free of infrastructure concerns like how those events actually get published (that's the outbox's job, Chapter 3).

### Alternatives and why they lose

- **Anemic domain model** (entities are plain data holders with getters/setters; all logic lives in a separate "service" class). Very common in practice, but it's really just transaction script with extra ceremony - invariants still aren't enforced at the source of truth, since nothing stops any service class from mutating the entity's fields in a way that violates a rule another service class assumed.
- **`AbstractAggregateRoot`-style event publishing** (the aggregate itself holds a list of pending events and a framework auto-publishes them). Convenient, but couples every aggregate to a specific infrastructure base class, and can hide *when* exactly publishing happens relative to persistence - the book's explicit `process()`-returns-events approach keeps that timing visible and testable.

### A concrete example

Before the refactor, `TicketService`/`SagaJoinService` were transaction scripts: a bag of procedural methods mutating raw fields with no guarded state machine - nothing prevented calling `markReady()` on a ticket that was never actually `ACCEPTED`. After the refactor, `Order` is a proper aggregate: it has real methods like `revise()` and `confirmRevision()`, its own state machine (you can't jump straight from `PENDING` to `DELIVERED` - the aggregate itself refuses the transition), and code elsewhere refers to a Consumer only by `consumerId` (a plain primary key), never by holding an actual `Consumer` object.

### What was actually built

All three of this project's DDD aggregates - `Ticket` (kitchen-service), `Order` (order-service), and `Authorization` (accounting-service) - were refactored into proper aggregates with enforced state transitions and domain events returned from their own methods, going one step beyond the book's own two named worked examples (`Ticket` and `Order`) by applying the same rigor to `Authorization` as well.

### Interview prep

**Q: What's the difference between a transaction script and a domain model, and what specifically breaks in production when you use a transaction script for something that needed a domain model?**
A transaction script puts one procedural method per operation directly against the data, with no entity enforcing its own rules; a domain model puts the rules on the entities themselves, so an invariant is enforced in one place regardless of caller. What breaks with a transaction script: two different call sites can each independently "forget" to check the same precondition (e.g. one code path lets an Order be modified after it's already `DELIVERED`, because that check only exists in the *other* code path) - a class of bug that's structurally impossible once the rule lives on the aggregate's own method instead of being duplicated (or half-duplicated) across callers.

**Q: Explain the three DDD aggregate rules and connect each one to a consequence elsewhere in the architecture.**
(1) Only reference the aggregate root from outside → lets the root be the single enforcement point for every invariant, since nothing can reach internal state around it. (2) Inter-aggregate references by primary key, not object reference → this is precisely what lets aggregates eventually live in *different services with different databases* (Chapter 2/3) without needing a cross-database object reference, which wouldn't work anyway. (3) One transaction touches exactly one aggregate → this is the direct reason sagas (Chapter 4) exist: any operation needing multiple aggregates updated together has to become a sequence of single-aggregate local transactions instead of one multi-aggregate transaction.

**Q: How do you decide aggregate granularity - one big Order aggregate covering everything vs. several small ones?**
It's a concurrency-vs-coordination trade-off. A fine-grained aggregate (e.g. keeping `Order` focused on order state and delegating delivery tracking to its own `Delivery` aggregate rather than folding it in) means unrelated concurrent updates (revising an order's items vs. updating delivery location) don't contend for the same row/lock, but now those two concerns need a saga (or at least an event) to stay coordinated instead of being trivially atomic together. A coarse-grained aggregate has the opposite trade: fewer sagas needed, but every unrelated concurrent update to the same aggregate now contends for the same lock, hurting throughput under load. The book recommends fine-grained by default and this project follows that (separate `Order`, `Ticket`, `Authorization`, each independently concurrent) - the general heuristic is: does this data change at a genuinely different rate or for a genuinely different reason than that data? If yes, they're probably separate aggregates.

**Q: Why does the book prefer aggregate methods returning `List<DomainEvent>` over a framework base class that auto-collects events?**
Returning events keeps the aggregate a plain domain object with zero infrastructure dependencies - it's trivially unit-testable (call the method, assert on the returned events, no framework or Spring context needed) and the *timing* of when events get published relative to persistence stays fully visible and explicit in the calling code (typically: call the aggregate method, get events back, persist the aggregate and write the events to the outbox table in the same transaction). A framework base class that auto-collects and later auto-publishes events hides that sequencing behind magic, which makes it harder to reason about exactly when (and whether) an event is guaranteed to have been persisted before it's published - exactly the kind of ambiguity the outbox pattern (Chapter 3) is designed to eliminate.

**Q: Give an example of an invariant that a domain model can enforce but an anemic model + service-layer validation cannot reliably enforce.**
Any invariant that depends on *multiple fields changing together* is fragile under anemic-model + setters: e.g. "an Order's total must always equal the sum of its line items" - with plain setters, code can update `lineItems` without updating `total`, and nothing prevents it, because the validation (if it exists at all) lives in some separate service method that not every code path necessarily calls. With the logic on the aggregate (`addLineItem()` recomputes `total` internally, and there's no public `setTotal()`), the invariant literally cannot be violated by any caller, because there's no code path that bypasses the aggregate's own logic.

---

## Chapter 6 - Event sourcing

### The book's idea

Instead of storing an aggregate's *current* state as a row you overwrite, event sourcing stores every state-changing event that ever happened to it, forever, in an append-only log - and rebuilds current state by replaying those events. This turns the event store into a hybrid of a database (it's your durable system of record - the events *are* the data, not a byproduct of it) and a message broker (via CDC on that same log, it's also how other services find out what happened, since every state change is already an explicit, published-able event by construction).

The discipline that makes this work is a strict split between two operations:

- **`process(Command)`** decides *what happened* - validates the command against current state, and if valid, produces the event(s) describing what happened. Pure decision-making; does not mutate anything.
- **`apply(Event)`** unconditionally mutates state given something that *already happened* - no validation, just apply the fact. The same `apply()` method runs both for a just-decided event (right after `process()` produces it) and for every historical event during a replay (rebuilding an aggregate from scratch). Collapsing these two into one step - deciding and mutating together - is the mistake that makes replay impossible, since replay needs to blindly re-apply history without re-validating it against a state it hasn't been rebuilt to yet.

**Snapshots** (a saved current-state checkpoint at a given event version) are purely a performance optimization - avoid replaying arbitrarily many events for an aggregate with a long history - and have no effect on correctness; an aggregate is always allowed to be rebuilt from event zero with the same result.

**Optimistic concurrency control** is still required with event sourcing (two concurrent commands against the same aggregate version must not both succeed) - it just moves from a `@Version` column on a mutable row to a version count on the event stream itself: a command is conditioned on "the aggregate is at version N," and the append fails if someone else already appended event N+1 first.

### Alternatives and why they lose

- **Plain CRUD persistence (a mutable current-state row).** Simpler to reason about for straightforward cases, but you lose the full history for free (no audit trail of *how* the current state was reached, only what it currently is) and you lose the natural, already-explicit event stream that CDC/outbox publishing needs - with CRUD, "what changed" has to be reconstructed after the fact (e.g. by diffing before/after state), whereas with event sourcing it's the primary artifact.
- **Storing snapshots only, no event log.** Defeats the entire purpose - you'd have current-state persistence with extra steps, but none of event sourcing's actual benefits (full history, natural event stream, ability to add new projections/read models later by replaying history you already have).

### A concrete example

Instead of a single `orders` table row that just says `status = APPROVED`, the event store holds a sequence like `OrderCreated`, `OrderAuthorized`, `TicketCreated` - and `Order`'s current state is whatever you get by replaying those in order. Rebuild the same Order from scratch tomorrow, replay the same events, get the identical result - that replayability is the whole point, and it also means a brand-new read model (say, "orders by hour of day for capacity planning") can be built later just by replaying history that was already being captured, with zero changes to how Order itself is persisted.

### What was actually built

A hand-rolled event-sourced persistence path for the `Order` aggregate: an `OrderEventStore`/`OrderAggregate` pair implementing the `process`/`apply` split, a snapshot mechanism, and a dedicated optimistic-lock version table (rather than deriving the version by counting rows, so the two concerns - how many events exist vs. what version an update is conditioned on - stay independently reasoned about), switchable against the regular JPA persistence path via a `PERSISTENCE_MODE` flag. It covers all three `Order` sagas (create, cancel, revise) in both saga styles, including the book's full orchestration-mode mechanism for publishing saga commands as pseudo-events (a `SagaCommandEvent`-style dedicated table, separate from the real event log, plus its own poller - deliberately built as genuine extra machinery rather than the "just publish in the same transaction" shortcut, specifically to see the book's actual mechanism).

A subtlety learned the hard way, not read: not every row that shares an event-sourced aggregate's storage table is automatically a replayable domain event - a wire-only pseudo-event smuggled onto the same table purely for CDC convenience will silently break every future replay unless the store explicitly distinguishes "happened to the aggregate" from "needs to reach Kafka." See `docs/ARCHITECTURE.md`'s "Event sourcing - `Order` aggregate (Ch.6)" section for the detailed mechanics.

### Interview prep

**Q: Explain event sourcing, and specifically the `process`/`apply` split - why does collapsing them break replay?**
`process(Command)` validates a command against the aggregate's *current* state and decides what event(s) resulted; `apply(Event)` unconditionally mutates state given an event that's already a fact. Replay needs to rebuild an aggregate purely from `apply()` calls over its history, in order, with no validation step - it's re-establishing a fact pattern, not re-deciding anything. If you collapse the two (validate-and-mutate in one step), replay either has nowhere to hook in without accidentally re-validating history against a not-yet-rebuilt state (which can spuriously reject valid historical events), or the mutation logic simply isn't separable from the decision logic at all, making a correct replay impossible to write.

**Q: When would you *not* use event sourcing for an aggregate?**
For aggregates with high write volume and a genuinely simple state machine where full history has no business or audit value, plain CRUD persistence is usually cheaper to build, easier for every engineer on the team to reason about, and doesn't require solving replay-performance (snapshots) or storage-growth problems. Event sourcing earns its complexity when you specifically need full auditability, the ability to reconstruct state as-of any point in time, or - the CQRS connection - a natural, already-structured event stream to drive new read models later without retrofitting change-capture onto a CRUD table. This project applied it to `Order` deliberately for the learning exercise, despite `Order`'s actually-short lifecycle not strictly needing it - a good interview answer should be explicit that this was a deliberate trade for learning value, not a claim that event sourcing was the objectively correct choice here.

**Q: What are snapshots for, and can an event-sourced system be correct without them?**
Snapshots are purely a performance optimization: instead of replaying every event from the beginning every time you need to load an aggregate, you load the most recent snapshot and replay only the events since it. A correctly implemented event-sourced system is fully correct without snapshots - snapshots change *how fast* you can rebuild state, never *what* the rebuilt state is. This project implemented snapshots for `Order` specifically to exercise the mechanism, even though `Order`'s short lifecycle means it would have been correct (just marginally slower to load) without them.

**Q: How does optimistic concurrency control work with an event-sourced aggregate, and why not just derive the version from `COUNT(*)` on the event table?**
A command is conditioned on "I'm operating against version N of this aggregate"; appending the resulting event(s) is only allowed if the aggregate is still actually at version N - if another command already appended first, the append is rejected and the caller must reload and retry. Deriving the version from `COUNT(*)` conflates two genuinely different concerns: how many events currently exist (a fact about the log) versus what version a given update is conditioned on (a concurrency-control concept) - collapsing them works until you need one to diverge from the other, e.g. if you ever need to compact, migrate, or reindex the event log without changing what "version" a pending command is checked against. A dedicated version-tracking table (this project's actual choice) keeps them independently correct by construction rather than by coincidence.

**Q: A teammate proposes storing an internal "wire-only" message directly in the event-sourced aggregate's own event table, purely because it's convenient for the outbox poller to pick up. What's your objection?**
Not every row an aggregate's storage table holds is necessarily a *domain* event describing something that happened to the aggregate - a message that only exists to get onto Kafka (e.g. a saga command pseudo-event) is a fundamentally different kind of thing from "this aggregate transitioned state," even if it's convenient to store them in the same table. If the event store doesn't explicitly distinguish the two, a future replay will blindly `apply()` the wire-only message as if it were real aggregate history, silently corrupting every subsequent rebuild - a bug that's easy to introduce and hard to detect, because it only manifests the next time someone actually replays from scratch. This is a real bug class this project hit while implementing orchestration-mode sagas over the event store, not a hypothetical.

---

## Chapter 7 - Implementing queries

### The book's idea

Once data is spread across several services' own databases (a direct consequence of Chapter 2's decomposition and Chapter 5's aggregate rules), how does a client get a single view that spans more than one of them? The book presents two genuinely different answers, not two versions of the same idea:

- **API composition**: assemble the response at request time, by calling each relevant service live and combining the results. Always as current as the data actually is right now, but the response is only as *available* as the least available service it calls, and only as *fast* as the slowest one it calls (unless calls are parallelized).
- **CQRS (Command Query Responsibility Segregation)**: maintain a separate, pre-built read model ahead of time, kept up to date incrementally by consuming the same domain events every service already publishes (a direct payoff of Chapter 3's messaging and Chapter 6's "events as a natural stream" idea). Reads are instant and fully decoupled from the write-side services' availability, at the cost of eventual (not immediate) consistency - the read model is always some small, bounded amount of time behind reality.

### Alternatives and why they lose

- **A shared database that every service can query directly for composed views.** The most tempting shortcut, and the one Chapter 2's decomposition explicitly rules out - it re-couples every service to a shared schema, defeats independent deployability (a schema change now needs cross-team coordination again), and defeats independent scaling (every service's query load now hits one shared database).
- **API composition for everything, including read-heavy dashboards/analytics.** Technically works, but at read-heavy scale it multiplies load on every underlying write-side service for every single read, and ties dashboard availability to the availability of every service it composes - exactly the profile CQRS is built to avoid for read-dominated access patterns.

### A concrete example

A customer wants to see their order's full status: the order itself, the restaurant's name, the kitchen's prep status, the payment status, and the courier's location. API composition means calling all four services in parallel right now and stitching the results together - always current, but only as available as the least available of the four, and only as fast as the slowest one. CQRS means a separate service has already been listening to every relevant event as it happened and keeps a single, ready-to-read row per order - instant to read, decoupled from those four services' availability, but only as fresh as the last event it processed (a moment behind reality, not a live snapshot).

### What was actually built

Both, deliberately, to see the trade-off firsthand rather than just read about it:

- **API composition**: `GET /orders/{id}/view` on order-service, fanning out to restaurant/kitchen/accounting/delivery-service in parallel via virtual threads, each guarded by its own circuit breaker (Chapter 3). A `SectionResult` type (`Found`/`NotFound`/`Unavailable`) means one slow or down dependency degrades only its own section of the response, not the whole thing - the direct fix to the "only as available as the least available service" problem, applied at the section level instead of accepting an all-or-nothing failure.
- **CQRS**: a new standalone `ftgo-order-history-service` maintains a denormalized `order_views` read model by consuming events from every relevant Kafka topic and upserting. It has no Eureka registration and answers `GET /order-views/{orderId}` with zero synchronous calls to anything, since its own availability must never depend on - or be depended on by - the write-side services. A real concurrency bug surfaced the actual cost of CQRS's "just consume events and upsert" simplicity: because `OrderView`'s `@Id` is externally assigned, every `save()` goes through `EntityManager.merge()` (a full-column UPDATE, not dirty-checked), so four independent `@KafkaListener` consumer threads racing to update the same row could silently lose an update without `@Version` optimistic locking - fixed by adding the same optimistic-locking pattern `ftgo-accounting-service`'s `SagaJoinState` already used for a different reason (exactly-once join resolution), now reused here for lost-update prevention across independent topics.

See `docs/ARCHITECTURE.md`'s "API composition" and "CQRS" sections for the sequence diagrams.

### Interview prep

**Q: API composition vs. CQRS - walk through the actual trade-off, not just the definitions.**
API composition reads live: it's always as current as the underlying services, but its availability is the *product* of every service it calls (call 4 services each 99.9% available in parallel, and the composed response's effective availability is bounded below by the least available of the four, worse if any call fails open rather than degrading gracefully), and it puts read load directly on the write-side services on every single request. CQRS reads from a pre-built model: reads are cheap, fast, and fully decoupled from write-side availability, but the read model is eventually consistent (some lag behind the latest write) and you now own a second data store that has to be kept correctly in sync - including handling out-of-order or duplicate event delivery. Pick API composition when freshness matters more than availability/latency and the composed calls are cheap/few; pick CQRS when read volume is high, read-side availability needs to be decoupled from write-side services, or the same data needs to be queried in shapes the write-side model doesn't naturally support.

**Q: How do you make API composition resilient to one of N downstream calls failing, without failing the whole request?**
Don't treat "get restaurant info," "get kitchen status," etc. as a single all-or-nothing operation - model each call's outcome explicitly (this project's `SectionResult`: `Found`/`NotFound`/`Unavailable`) and combine them into a response where a failed section is visibly marked unavailable rather than aborting the whole composed response. Combine this with per-call circuit breakers (Chapter 3) so a persistently failing downstream doesn't even get called on every request, and with parallel (not sequential) fan-out so the composed response's latency is bounded by the *slowest* call, not the *sum* of all calls.

**Q: What does "eventually consistent" actually mean operationally for a CQRS read model, and what's a concrete failure mode it introduces that a synchronous read never has?**
It means there's a real, non-zero window during which the read model doesn't yet reflect a write that already committed on the write side - a customer could refresh a page immediately after placing an order and briefly see stale (or momentarily missing) data in the CQRS view, even though the write genuinely succeeded. A concrete failure mode: out-of-order or concurrent event delivery to the read model can produce a read model that reflects events applied in the wrong order relative to how they actually happened, if the consumer doesn't defend against it - which is exactly the bug this project hit (`OrderView` rows getting silently overwritten by a stale update from a slower consumer thread) before adding `@Version`-based optimistic locking to make "apply this update only if it's still the version I expect" an explicit, enforced rule rather than an assumption.

**Q: Why did upserting into the CQRS read model need optimistic locking, when a naive read of the code might assume "just consume events and save, what could go wrong"?**
Because multiple independent Kafka topics (order.events, kitchen.events, accounting.events, delivery.events) are each consumed on their own listener thread, and more than one of those threads can race to update the *same* `OrderView` row concurrently. With an externally-assigned `@Id`, JPA's `save()` goes through `merge()` - a full-column UPDATE that isn't dirty-checked against what was actually read - so a slower thread's update can silently overwrite a faster thread's more-recent update with stale data, with no error raised anywhere. Adding `@Version` makes that overwrite fail loudly (an optimistic-locking exception) instead of silently, so the consumer can detect it and retry against the now-current row instead of losing data invisibly - the same fix pattern this project's own accounting-service already used for a different reason (exactly-once saga-join resolution), reapplied here for a different but structurally similar concurrency hazard.

**Q: At what read volume or team size does building a dedicated CQRS read-model service become worth the operational cost of a second data store and a new service to run?**
There's no fixed number, but the signal to watch for is: is API composition's load on your write-side services (or its tail latency, driven by the slowest composed call) becoming a genuine availability or scaling problem for those write-side services themselves - not just "reads are a bit slow." If write-side services are otherwise healthy and read traffic is low-to-moderate, API composition's simplicity (no second data store, no consistency lag to reason about) usually wins. Once read traffic materially competes with write traffic for the same services' capacity, or a client needs a query shape (e.g. "all orders in the last hour across all restaurants") the write-side aggregates were never designed to answer efficiently, that's the point CQRS's dedicated, purpose-built read model starts paying for its own operational cost.

---

## Chapter 8 - External API patterns

### The book's idea

Client applications (a mobile app, a public partner API) shouldn't talk directly to a dozen backend services - there needs to be a single front door. The book presents this as one underlying pattern (an **API gateway**: an edge service that routes, and sometimes composes, requests to the services behind it, centralizing cross-cutting edge concerns like auth and rate limiting) applied with two different ownership choices:

- **A single shared gateway for every client.** Simpler to run (one thing to deploy and operate), but every client's needs now compete for the same gateway's routes, filters, and release schedule.
- **Backends for Frontends (BFF)**: a separate gateway per client type, each independently owned, configured, and released for that client's specific needs.

### Alternatives and why they lose

- **No gateway at all - clients call backend services directly.** Exposes internal service topology to every client (a service split or rename becomes a client-facing breaking change), duplicates cross-cutting concerns (auth, rate limiting, logging) into every client instead of centralizing them once, and gives up the ability to compose/optimize responses for a specific client's needs (e.g. a mobile client wanting one round trip instead of four).
- **One shared gateway when client needs genuinely diverge.** A mobile app typically wants a few tailored, composed, low-round-trip endpoints; a public partner API typically wants broad, generic routes with its own API-key and rate-limit rules. Forcing both through one gateway means every change has to satisfy both sets of needs at once, and the two teams end up coordinating releases through a shared component neither of them fully owns - a smaller-scale repeat of the exact coordination cost microservices was meant to eliminate (Chapter 1).

### A concrete example

A mobile app typically wants a few tailored, composed endpoints (minimize round trips over a slow, high-latency mobile connection); a public partner API typically wants broad, generic routes with its own API-key and rate-limit rules, since partners are external and need to be metered and isolated from each other. Giving them the same gateway means every change has to satisfy both sets of needs at once and coordinate two teams' release schedules through one shared component; giving each its own gateway lets the mobile team ship changes (like a new composed endpoint) without coordinating with the partner-API team at all.

### What was actually built

The BFF variant: `ftgo-mobile-gateway` (routing plus one hand-composed endpoint, `GET /mobile/orders/{orderId}`) and `ftgo-public-gateway` (pure routing to six backend services), each independently configured (its own API key, its own rate limit), sharing common edge behavior (request logging, API-key auth) via a shared `ftgo-gateway-common` library so cross-cutting concerns aren't duplicated even though ownership is split.

A real, non-obvious finding from building this: Spring Cloud Gateway's filter chain (`GlobalFilter`s, route-level `GatewayFilterFactory`s) only applies to requests matched by a declared `RouteLocator`/YAML route - a hand-written WebFlux `RouterFunction` bean in the same application is dispatched by a completely different mechanism (`RouterFunctionMapping`) and never sees that filter chain at all, so any cross-cutting concern (auth, logging, rate limiting) needed on a RouterFunction-based endpoint has to be applied by hand, endpoint by endpoint - a real operational gap this project deliberately left parked (no rate limiting/logging on the mobile gateway's composed endpoint) rather than silently ignoring it. See `docs/ARCHITECTURE.md`'s "API Gateway / Backends for Frontends" section.

### Interview prep

**Q: Why put an API gateway in front of microservices at all, instead of clients calling services directly?**
Three reasons that compound: it hides internal service topology from clients, so a service split, merge, or rename doesn't become a breaking change for every client that has to be coordinated separately; it centralizes cross-cutting edge concerns (authentication, rate limiting, request logging) in one place instead of duplicating them into every service or every client; and it's the natural place to do request composition for a specific client's needs (e.g. one round trip instead of four for a mobile client), which is architecturally the same idea as Chapter 7's API composition, just performed one layer further out, at the edge rather than inside one of the composed services.

**Q: When do you split into multiple BFFs instead of one shared gateway, and what's the actual cost of splitting too early?**
Split when client needs genuinely diverge in ways that would otherwise force two teams to coordinate changes through one shared component (different composition needs, different auth/rate-limit policies, different release cadences) - this project's mobile vs. public-partner split is a real example, not a hypothetical. The cost of splitting too early (e.g. a BFF per client before there are genuinely differentiated needs) is duplicated infrastructure and cross-cutting logic with no actual benefit yet - which is why this project factored the genuinely shared behavior (auth, logging) into `ftgo-gateway-common` rather than duplicating it into each gateway, capturing the ownership benefit of splitting without paying for duplicated cross-cutting code.

**Q: Explain the RouterFunction-vs-declared-route filter-chain gap this project found, and why it's the kind of bug that's easy to miss in code review.**
Spring Cloud Gateway's filter chain is attached specifically to requests matched by a declared route (`RouteLocator` beans or YAML config) - it's not a global interceptor over every request the application handles. A hand-written WebFlux `RouterFunction` endpoint defined in the same Spring application is matched and dispatched by an entirely separate mechanism (`RouterFunctionMapping`), so it silently bypasses every gateway filter - no auth, no rate limiting, no request logging - unless that logic is added by hand on that specific endpoint. It's easy to miss in code review because both endpoint styles look equally "part of the gateway" from the outside (same application, same port, same base URL) - the divergence is purely about *which internal dispatch mechanism* handles the request, which isn't visible without knowing Spring Cloud Gateway's internals. The general lesson: whenever a framework offers two different ways to define an endpoint, verify explicitly that cross-cutting infrastructure (filters, interceptors, auth) actually applies to both, rather than assuming it does because they're "in the same app."

**Q: How would you decide what belongs in an API gateway/BFF versus what belongs in the underlying services?**
As a rule of thumb: put concerns that are about the *edge of the system* - authentication of external callers, per-client rate limiting, request/response shaping for a specific client type, and composition purely for a specific client's convenience - at the gateway. Keep business logic, domain validation, and anything that needs to be true regardless of which client is calling, inside the owning services - a gateway that accumulates business logic becomes a second, uncoordinated place business rules can live and drift out of sync with the services that are supposed to own them, which is its own version of the distributed-monolith risk from Chapter 1.

---

## Chapter 9 - Testing microservices: Part 1

### The book's idea

The **test pyramid** says: many fast unit tests at the base, fewer slower integration-style tests above that, and very few full end-to-end tests at the top - not because end-to-end tests are less valuable, but because they're slow, expensive to maintain, and brittle (a single flaky downstream service can fail a test that has nothing to do with the bug being hunted), so you want the vast majority of bugs caught by the cheap, fast layer instead.

Within unit testing, the book distinguishes two testing styles by what they exercise:

- **Sociable unit tests**: test a class together with its real, cheap-to-construct collaborators - appropriate for entities, value objects, and sagas, where the "collaborators" are just other plain domain objects with no external dependencies worth mocking.
- **Solitary unit tests**: mock the collaborators and test just this class's own logic in isolation - appropriate for domain services, controllers, and message/event handlers, where the collaborators are real external dependencies (repositories, publishers, other services) that would make the test slow, flaky, or simply not a test of *this* class's logic if left real.

### Alternatives and why they lose

- **Mock everything, always (fully solitary tests for entities too).** Produces tests that pass even when the entity's actual business logic is wrong, because the assertions only ever check that some mocked collaborator method was called, never that the resulting domain state is actually correct - false confidence.
- **Never mock anything (fully sociable tests for controllers/services too).** For a controller or event handler, this means every unit test now needs a real repository, a real database, possibly a real message broker - turning what should be a fast, isolated unit test into a slow integration test in disguise, defeating the point of having a fast base layer at all.

### A concrete example

Testing the `Order` aggregate's `revise()` method by constructing a real `Order` through its own methods and asserting on its resulting state is a *sociable* test - there's nothing worth mocking, the "collaborator" is just the aggregate's own cheap-to-build state, and mocking would only hide whether the actual business logic is correct. Testing a controller that calls out to `OrderService` is a *solitary* test - you mock `OrderService` and assert the controller called it with the right arguments and handled its response correctly, without actually running real business logic underneath (that's `OrderService`'s own test's job).

### What was actually built

This project's existing test suite was audited against the book's six named unit-testing techniques and already matched four of them independently - evidence that the DDD-aggregate work from Chapter 5 had already been quietly shaping the tests toward this structure, since a well-designed aggregate almost naturally invites sociable testing and a well-designed controller/service split almost naturally invites solitary testing. The two real gaps found were about assertion *depth*, not technique: several saga-orchestrator and message-handler tests checked that a call happened but not that its payload was correct (a trailing Mockito `any()` where a real, specific value was available and should have been asserted) - tightened across the relevant test classes, plus a new standalone value-object test for `OrderLineItem` added as a worked example of sociable testing at the smallest possible scale.

### Interview prep

**Q: Explain sociable vs. solitary unit tests, and give a concrete rule for which one a given class needs.**
The rule: if the class's collaborators are cheap, deterministic, and side-effect-free (other domain objects, value objects), test it sociably - construct real collaborators, because mocking them would hide whether the actual logic is correct and gains nothing in speed or reliability. If the class's collaborators are external dependencies (a repository hitting a real datastore, a publisher hitting a real broker, another service over the network), test it solitarily - mock them, because leaving them real makes the test slow, flaky, and no longer isolated to this class's own logic. Entities/value objects/sagas fall in the first bucket; domain services/controllers/event handlers fall in the second.

**Q: What's the actual risk of a Mockito `any()` in an assertion, versus asserting on the specific expected value?**
`verify(mock).method(any())` only proves the method was called *some* way - it can't catch a bug where the method is called with the *wrong* payload (a field mapped incorrectly, a status set to the wrong value, an amount computed wrong). This is exactly the class of blind spot this project found and fixed: several saga-orchestrator and message-handler tests verified a call happened but not that its arguments were correct, meaning a real payload bug (e.g. a wrong-field regression) could ship with green tests. The fix is mechanical but important: assert on the actual expected value (`verify(mock).method(eq(expectedOrder))` or an argument captor with real assertions), not just that a call of the right shape happened.

**Q: Why might a codebase converge on the test pyramid's structure without anyone deliberately reading the book chapter first?**
Because good architecture and good testability are the same underlying discipline viewed from two angles - a well-factored DDD aggregate (Chapter 5) with behavior on the entity naturally invites sociable, dependency-free tests, and a clean domain-service/controller/handler separation naturally invites solitary tests with clear mock boundaries. This project's own experience bears this out directly: auditing the existing suite against the book's six named techniques found 4 of 6 already present, purely as a side effect of the architectural decisions made in earlier chapters, not because the test suite was deliberately designed around the pyramid from the start.

**Q: How do you convince a team to invest in more unit tests when end-to-end tests "already cover everything"?**
Make the cost asymmetry concrete rather than arguing principle: an end-to-end test failure due to a bug three services deep can take significant time just to *localize* (which of the several services in the chain actually has the bug?), whereas a unit test failure points directly at the broken class and often the broken line. Also make the *coverage* gap concrete - end-to-end tests can only practically exercise a small number of critical-path scenarios (this project deliberately kept its end-to-end suite to one comprehensive journey, Chapter 10) precisely because they're expensive to write and slow to run, so most edge cases and failure branches (a rejected saga step, an invalid state transition) are only realistically covered at the unit level; if they're not covered there, they're very likely not covered anywhere.

---

## Chapter 10 - Testing microservices: Part 2

### The book's idea

Above unit tests in the pyramid, three more layers, each catching a different class of bug:

- **Consumer-driven contract tests**: the consumer of an API defines the shape of interaction it expects (a "contract" - example request/response pairs), and the producer is automatically verified against that contract in CI, without either service's full stack running against the other. Catches breaking API changes (a renamed or removed field a real consumer depends on) *before* either service is ever deployed, at a fraction of the cost of a full integration test.
- **Component tests**: spin up one service's real internal wiring - controllers through domain logic through persistence and messaging - against real, disposable infrastructure (e.g. via Testcontainers), but with every *other* service stubbed out. Tests that one service's own wiring is correct in isolation, without the cost or flakiness of running the entire multi-service system.
- **End-to-end tests**: drive the real, fully deployed system exactly the way an actual client would, with no stubbing anywhere. The most realistic layer and the only one that observes genuine cross-service behavior from the outside - but also the slowest and most brittle, so the book is explicit that these should be very few in number, reserved for the handful of scenarios that matter most.

### Alternatives and why they lose

- **Skip contract tests, rely on end-to-end tests to catch integration bugs.** Technically catches the same class of bug eventually, but far later (only once a full deploy happens) and far more expensively (a slow, flaky end-to-end run instead of a fast, isolated contract check in each service's own CI pipeline) - and it doesn't localize the failure to which service actually broke the contract.
- **Component tests against a shared, always-on test environment instead of disposable per-run infrastructure.** Seems cheaper up front, but shared test environments accumulate cross-test state pollution and become a shared, contended resource across the whole team - exactly the kind of coupling microservices testing strategy is trying to avoid, now reintroduced at the test-infrastructure layer.

### A concrete example

A contract test between `ftgo-mobile-gateway` (consumer) and `ftgo-order-service` (producer) fails the build the moment order-service renames a field the gateway depends on - *before* anyone deploys anything, and without either service's real database or message broker running at all. A component test for order-service spins up order-service for real (with a real, disposable MySQL and Kafka via Testcontainers) but stubs out every *other* service, checking order-service's own wiring is correct in isolation, fast and repeatable. An end-to-end test does neither shortcut: it drives a full Create → Revise → Cancel Order journey through the real public gateway against the entire Docker Compose stack, exactly as a real client would experience it - the only test in the whole suite that can catch a bug in how services genuinely coordinate with each other in production-like conditions.

### What was actually built

All three levels, covering the `Order` sagas:

- **Consumer-driven contracts** via Spring Cloud Contract Verifier: a REST contract (mobile-gateway ↔ order-service), a pub/sub contract (order-service → order-history-service), and an async request/response contract (order-service ↔ kitchen-service), including a hand-written embedded-Kafka bridge (`KafkaContractTestSupport`) for the two Kafka-based contracts, since this project isn't on Spring Cloud Stream/Eventuate Tram and there's no off-the-shelf messaging integration for that combination.
- **Component test**: an out-of-process Cucumber suite driving the real, containerized order-service through its Place Order flow (orchestration mode, JPA persistence) against a slimmed Docker Compose stack, with restaurant-service stubbed via WireMock and the four saga participants stood in for by a single Kafka-based `SagaParticipantStub`.
- **End-to-end test**: a new `ftgo-end-to-end-test` module drives one Cucumber scenario - Create → Revise → Cancel Order - through the real, full-stack application (all 7 business services + both gateways, unmodified root `compose.yml`, `SAGA_MODE=orchestration`) entered via `ftgo-public-gateway`, exercising all three `Order` sagas in one journey. A domain detail this scenario had to get exactly right: `Order.rejectRevision()` returns status to `APPROVED`, not `REJECTED` (that status is reserved for a declined *initial* CreateOrder authorization), so the test asserts on line-item quantity reverting, not on status, to correctly detect a declined revision.

See `docs/ARCHITECTURE.md`'s "End-to-end testing" section.

### Interview prep

**Q: Explain consumer-driven contract testing, and why it's cheaper than catching the same bug with an end-to-end test.**
The consumer (the service that calls an API) defines a contract - concrete example requests and the responses it expects - and the producer's CI pipeline replays that contract against the producer in isolation, with no consumer service actually running. A breaking change (a renamed field, a removed endpoint) fails the *producer's own build*, immediately, with a precise pointer to exactly what broke - versus an end-to-end test, which would only catch the same bug after both services are actually deployed together, takes far longer to run, and (because it exercises the whole system) doesn't by itself tell you which of several services actually caused the failure.

**Q: Component tests vs. end-to-end tests - what does each catch that the other can't?**
A component test verifies one service's own internal wiring (controller → domain logic → persistence → outbox → message broker) is correct, using real infrastructure for that one service but stubbing every other service - it can't catch a bug in how two real services actually coordinate, because the other side is fake by design. An end-to-end test is the only layer that observes genuine cross-service coordination (real saga execution across real services) from the outside, exactly as a client would - but it can't efficiently localize *which* service caused a failure, and its cost means you can only afford a small number of scenarios, so it structurally can't provide the coverage depth unit and component tests can.

**Q: The book says end-to-end tests should be very few in number. How do you decide which scenarios actually earn a spot in that small set?**
Pick scenarios that specifically exercise genuine cross-service coordination that no lower layer can observe - not scenarios that are really just testing one service's logic (which belongs at the unit or component level). This project's single end-to-end scenario was deliberately chosen to be a full saga lifecycle (Create → Revise → Cancel) run under `SAGA_MODE=orchestration`, specifically because the choreography path was already covered by earlier chapters' more targeted verification, and orchestration hadn't yet been exercised under true full-stack, real-network conditions - the goal was maximum genuinely-new coverage per expensive test, not just "one more happy path."

**Q: You're debugging a subtle saga state bug and have to choose which test scenario to write it into. Walk through your reasoning for picking status vs. quantity as the assertion.**
Assert on whatever field the business logic actually changes as its *primary, unambiguous* signal of the outcome you're testing - not on a field that happens to be adjacent but is reused for a different purpose. This project's revise-decline scenario is the concrete case: a declined revision returns `Order.status` to `APPROVED`, the same status a never-revised, already-approved order also has - so asserting on status alone can't distinguish "revision was declined" from "revision was never attempted." The actually distinguishing signal is that the line items reverted to their pre-revision quantity, so the test asserts on quantity, not status - a reminder to trace exactly what a state transition does and doesn't uniquely signal, rather than assuming the most obvious-looking field is the right one to assert on.

**Q: How would you structure a CI pipeline around these four testing layers (unit, contract, component, end-to-end) to get fast feedback without sacrificing coverage?**
Run unit and contract tests on every commit/PR - both are fast (contract tests don't need the consumer service running at all) and should gate merges. Run component tests on every PR too, or at minimum pre-merge to main, since they're slower (real disposable infrastructure) but still scoped to one service and reasonably fast. Run the small end-to-end suite less frequently on the fast inner loop - typically pre-release or on merge to main/a deploy branch - since it needs the entire stack up and is the slowest, most expensive layer; gating every single commit on it would slow the team down disproportionately to the marginal bugs it catches beyond what the other three layers already caught.

---

## Chapter 11 - Developing production-ready services

### The book's idea

A service isn't actually production-ready just because its business logic works. It also needs identity/permission verification (an **authorization server** issuing tokens, every service checking them), **externalized configuration** (config that lives outside the deployed artifact, so it can change without a rebuild/redeploy), and observability - **health checks**, **application metrics**, **distributed tracing**, **log aggregation**, and **exception tracking** - plus **audit logging** (a record of who did what, and whether it succeeded, distinct from the domain's own event history). The book packages the idea of "every service needs all of this, consistently" as a **microservice chassis** (§11.4) - a shared foundation new services are built on, so production-readiness isn't something each team has to reinvent per service.

The chapter's real theme, learned directly by building it rather than just read: production-readiness is almost entirely *cross-cutting*. Every one of these concerns has to reach every service, so the interesting engineering question each time isn't "what does this pattern do" (that part is usually simple), it's "how does it reach every service consistently, without every team having to remember to wire it up by hand." Three distinct mechanisms answer that "how," and picking the right one for a given concern matters:

1. **Shared library code, auto-registered** (e.g. via Spring's `AutoConfiguration.imports`) - no service opts in by hand; the moment a service depends on the shared library, the behavior is active.
2. **A shared build configuration block** applied uniformly across every service's build file - dependencies and plugin configuration are consistent by construction, not by convention.
3. **Pure infrastructure-level collection**, requiring nothing at all from the service's own code (e.g. a log shipper scraping stdout, a metrics scraper hitting a well-known endpoint) - the most robust option where it's available, since it can't be forgotten or misconfigured per-service.

### Alternatives and why they lose

- **Let each service team implement observability/security independently.** Guarantees inconsistency - different services will end up with different log formats, different auth-checking logic (some subtly wrong), different metrics naming, making cross-service correlation (the actual payoff of observability) far harder or outright impossible.
- **A single "do everything" base class every service must extend.** Works, but couples every service's business code to a specific inheritance hierarchy for what are fundamentally cross-cutting, not domain, concerns - auto-registered library code (mechanism 1 above) gets the same consistency without that coupling.

### A concrete example

Distributed tracing only becomes useful once a single request's `traceId` shows up consistently in every log line and every metric it touches, across every service it passed through - a trace that stops at one service's boundary, or a log line with no `traceId` at all, can't be correlated with anything and might as well not exist. That correlation, not any single tool in isolation, is the actual payoff of this chapter: a real production incident is diagnosed by following one `traceId` from a slow customer-facing request, through Kibana's logs, into Tempo's trace spans, service by service, not by staring at any one service's dashboard alone.

### What was actually built

All of it, delivered through the three cross-cutting mechanisms above depending on what fit best:

- **Security**: a new `ftgo-authorization-server` (OAuth2 Authorization Server) issues JWTs via a custom resource-owner-password grant (end users) and a `client_credentials` grant (order-service's service-to-service calls); both gateways and all 8 business services are OAuth2 resource servers with `@PreAuthorize` role requirements; order-service additionally enforces an instance-based ACL (`OrderAccessControl` - a `CONSUMER` can only see their own order, not just any order of the right shape).
- **Externalized configuration**: `ftgo-config-server` (Spring Cloud Config Server, port 8888), git-backed, with shared defaults plus per-service overrides for the 5 outbox services, a non-blocking `fail-fast: false` startup contract (a service can start even if the config server is briefly unavailable), and live refresh of `outbox.poll-fixed-delay-ms` via `POST /actuator/refresh` with no redeploy.
- **Health checks**: `/actuator/health` on all 10 services (auto-configured indicators only), driving Compose `healthcheck`/`depends_on: service_healthy` gates so dependent services don't start before their dependencies are actually ready.
- **Application metrics**: Micrometer + Prometheus (`/actuator/prometheus` on all 10 services, custom business counters on 7), a Grafana dashboard, and alert rules.
- **Distributed tracing**: Micrometer Tracing + OpenTelemetry (replacing the deprecated Sleuth/Zipkin stack) on all 10 services, OTLP/HTTP export to Grafana Tempo at 100% sampling, explicit Kafka observation properties on the Kafka-touching services (a project-wide gap found during final review: `ftgo-common`'s hand-built `KafkaTemplate` bean silently bypassed producer-side tracing unless explicitly enabled), and explicit Reactor context propagation for both gateways (not on by default).
- **Log aggregation**: ELK + Filebeat; all 10 services log structured JSON to stdout with `traceId`/`spanId` from the tracing MDC, indexed as `ftgo-logs-*` and correlatable with Tempo traces in Kibana.
- **Exception tracking**: GlitchTip (Postgres + Redis-backed), with a one-shot provisioner bootstrapping the org/project/DSN into a shared volume; `sentry-spring-boot-starter-jakarta` on all 10 services captures only uncaught exceptions; deliberately not correlated with Tempo/Kibana `traceId`s (a known, documented gap, not an oversight).
- **Audit logging**: `ftgo-common`'s `AuditLoggingAspect` (`@Around`, matching `@PostMapping`+`@PreAuthorize` controller methods, registered by auto-configuration so no service opts in by hand) publishes `AuditLogEntryEvent`s best-effort to a Kafka `audit-log` topic; a new `ftgo-audit-log-service` consumes them into an append-only, `ADMIN`-only queryable ledger. Two honest, documented gaps rather than papered over: `POST /restaurants` is unaudited (restaurant-service is the one business service without an `ftgo-common` dependency), and publishing is best-effort rather than routed through the transactional outbox, so an audit record can theoretically be lost while its business transaction still commits.

See `docs/ARCHITECTURE.md`'s Chapter 11 sections for the full technical detail on each sub-pattern.

### Interview prep

**Q: What's a "microservice chassis," and why does the book treat it as its own pattern rather than just "do all the production-readiness things"?**
A chassis is the shared foundation every new service is built on, so cross-cutting production-readiness concerns (security, config, observability) are consistent by construction rather than something each team has to remember and implement correctly on their own, every time. It's treated as its own pattern because *inconsistency* here has a specific, expensive cost: if service A logs in a different format than service B, or checks auth slightly differently, cross-service correlation and security review both become much harder - the chassis's value isn't any single capability, it's guaranteeing the same capability is implemented identically everywhere.

**Q: Give three different mechanisms for applying a cross-cutting concern to every service, and the trade-off of each.**
Auto-registered shared library code (e.g. Spring's `AutoConfiguration.imports`) requires zero opt-in from each service once the dependency is present, but the mechanism itself needs to be well understood by the team, since it's easy to assume something is active everywhere without verifying it (this project's own Kafka-tracing gap - a hand-built `KafkaTemplate` bean silently bypassed an auto-configured tracing property - is exactly this failure mode). A shared build-config block applied uniformly guarantees dependency/plugin consistency at build time, but doesn't itself guarantee runtime behavior is correct - two services with identical dependencies can still be wired up differently in application code. Pure infrastructure-level collection (a log shipper, a metrics scraper) is the most robust, since it requires nothing from the service code at all and so can't be individually forgotten - but it only works for concerns that are genuinely observable from outside the process (you can't get application-specific business metrics this way, only what's externally visible).

**Q: Explain why `traceId` correlation is "the actual payoff" of observability, more than any individual tool.**
Each individual signal - a log line, a metric spike, a trace span - tells you *that* something happened somewhere, but not necessarily the full causal story. A `traceId` present consistently in logs, traces, and (ideally) exception events lets you start from any one signal (e.g. a slow customer request) and follow the *same request* across every service it touched, in every tool, turning three separate, siloed views into one coherent investigation. This project's exception tracking (GlitchTip) deliberately isn't correlated with `traceId` yet, which is called out explicitly as a real, current limitation - an exception in GlitchTip can't be directly linked to the trace or logs for the same request, which is a genuine investigative gap versus the fully correlated log/trace pairing.

**Q: Walk through the Kafka tracing gap this project found - what happened, and what's the general lesson?**
`ftgo-common` had a hand-built `KafkaTemplate` bean (rather than relying on Spring Boot's auto-configured one), and that hand-built bean didn't have the auto-configured observation/tracing property wired onto it - so every service using it was silently missing producer-side Kafka spans in its traces, project-wide, without any error or obvious symptom. It was caught during a final whole-branch review, not by any automated check. The general lesson: any time application code manually constructs a bean that a framework would otherwise auto-configure for you (often done for a legitimate reason - custom serialization, custom error handling), explicitly verify that cross-cutting behavior the auto-configuration would normally provide (here, tracing instrumentation) is still present on the hand-built version - it's not carried over for free just because the bean serves the same functional purpose.

**Q: Why is audit logging a genuinely different concern from event sourcing, even though both produce an append-only history?**
The event store (Chapter 6) records what the *domain decided* - state transitions the aggregate itself validated and committed to, only ever successful outcomes, since a rejected command never produces a domain event. The audit log records *who invoked what and whether it succeeded*, including failed and unauthorized attempts, which never become domain events at all - a rejected `POST /orders/{id}/revise` call because the caller wasn't authorized is audit-log-worthy (someone tried something) but produces zero domain events (nothing actually happened to the Order). They're both append-only histories, but they're recording fundamentally different facts, for different consumers (domain events for other services' business logic; audit logs for security/compliance review).

**Q: The audit log publishes best-effort rather than through the transactional outbox - is that a bug, and how would you decide whether to fix it?**
It's a known, deliberately documented trade-off, not an oversight: routing audit events through the outbox would make them exactly as durable as business events (guaranteed to be published if the business transaction committed), but it would also couple the audit-logging aspect to each service's specific `OutboxEvent` table and transaction boundary, adding real coupling and complexity to what's currently a lightweight, decoupled cross-cutting concern. Whether to fix it is a genuine product/compliance question, not a purely technical one: if audit records need to be legally or contractually guaranteed durable (e.g. for a compliance audit trail), the outbox is worth the added coupling; if occasional best-effort loss of an audit record is an acceptable risk relative to the operational simplicity gained, the current design is defensible as-is. A staff engineer's job here is making that trade-off explicit to the people who own the compliance requirement, not silently picking one side.

---

## Chapter 12 - Deploying microservices

### The book's idea

Getting services running reliably in production is its own set of patterns: packaging each service as a **container** (self-contained, runs the same way everywhere, regardless of the host's own installed dependencies), running a fleet of them with an orchestration platform like **Kubernetes** (handles scheduling, restarts, rolling updates, and service-to-service networking primitives), and - once you have enough services talking to each other - a **service mesh**: a layer of per-pod network proxies (**sidecars**, since each one sits alongside its application container in the same pod, intercepting all its traffic) that transparently add encryption, retries, and traffic-shaping to every request, without the application code knowing or needing to change.

### Alternatives and why they lose

- **VM-based deployment instead of containers.** Containers share the host OS kernel (fast to start, small image size, consistent behavior across environments); VMs virtualize the whole OS (heavier, slower to start, more resource overhead per instance) - for a fleet of many small services, container density and startup speed matter a lot more than VM-level isolation strength, which most services don't actually need.
- **Every service implements its own TLS, retries, and traffic-shaping in application code.** Works, but duplicates non-trivial, security-sensitive logic across every service in every language, and makes a fleet-wide policy change (e.g. "require mTLS everywhere now") a coordinated multi-service code change instead of a mesh-level configuration change. A service mesh centralizes this exactly the way Chapter 3's circuit breaker centralizes resilience logic, but at the infrastructure layer instead of in a shared library.

### A concrete example

Without a service mesh, if you want every service-to-service call encrypted, every service has to implement TLS itself - key management, cert rotation, and the actual handshake logic, replicated in every service. With a mesh (this project uses Linkerd), a sidecar proxy is injected into every pod, and it automatically encrypts (mTLS) and can retry every outbound call - the application code is completely unaware it's even happening, and a fleet-wide policy change is a mesh configuration change, not a multi-service code change.

### What was actually built

- **§12.1-12.3 (sub-project A)**: Docker images and Docker Compose for the whole stack - see `docs/CH12-DEPLOYMENT.md` for the full packaging details.
- **§12.4 sub-project B1**: the entire stack redeployed to a local Kubernetes cluster (`kind`) via a Helm chart, verified with the end-to-end test suite running against it through a Kubernetes-profile base URL.
- **B2**: zero-downtime rolling deployments.
- **B3a**: a Linkerd service mesh providing automatic mutual TLS between every pod, with no application-code changes.
- **B3b**: mesh observability (a linkerd-viz golden-metrics dashboard).
- **B3c**: mesh traffic management (`ServiceProfile`s authored for order-service's downstream GET routes) - and the chapter's most significant finding, discovered only through live fault-injection testing, not by reading the configuration: those `ServiceProfile`s have **zero effect** on order-service's real traffic. Linkerd's traffic-management features are keyed on a request resolving to a Kubernetes Service's `<name>.<namespace>.svc.cluster.local` identity, not on which pod ultimately receives the bytes - but `order-service`'s `@LoadBalanced` `RestClient`s resolve Eureka application names straight to a pod IP (a Chapter 3 decision, made long before the mesh existed), so Linkerd never sees a Service-addressed request to attach a `ServiceProfile`'s retry behavior to, no matter how correctly the `ServiceProfile` is authored. Confirmed empirically: identical 200-with-degraded-section fault behavior, and zero route-classified outbound metrics, whether the `ServiceProfile` was present or deleted. This is a genuine architectural finding about mesh vs. client-side service discovery being two *independent* implementations of the same underlying concern (locate a healthy backend instance) that don't automatically combine their benefits when stacked - not an implementation defect in either layer.

See `docs/ARCHITECTURE.md`'s "Kubernetes deployment" section for the full picture, including this finding's supporting evidence.

### Interview prep

**Q: What does a service mesh actually give you that a well-built application (with, say, its own circuit breakers and retries, per Chapter 3) doesn't already have?**
Centralization and uniformity without application-code changes: every service gets mTLS, retries, and traffic-shaping identically, enforced at the infrastructure layer, so a fleet-wide policy change (or a security requirement like "everything must be mTLS now") is a mesh configuration change rather than a coordinated multi-service, multi-language code change. What it doesn't give you for free is *business-logic-aware* resilience (Chapter 3's `SectionResult`-style graceful degradation, which needs to know what a "degraded but still useful" response means for this specific API) - the mesh and application-level resilience patterns are complementary, not substitutes for each other.

**Q: Explain this project's own finding: why did the Linkerd `ServiceProfile`s have no effect on order-service's traffic, and how was that actually verified rather than assumed?**
A service mesh's traffic-management features attach to requests based on the *Kubernetes Service identity* the request is addressed to (`<name>.<namespace>.svc.cluster.local`) - that's the hook the mesh's sidecar proxy uses to know which policy applies. `order-service` resolves its downstream calls via Eureka (client-side discovery, a decision from Chapter 3, made before the mesh was ever introduced), which resolves an application name straight to a specific pod's IP address - never routing through the Kubernetes Service abstraction at all. So from the mesh's point of view, that outbound traffic was never Service-addressed, and a `ServiceProfile` configured for that Service has nothing to attach to. This wasn't inferred from reading the CRD schema or the mesh's docs - it was verified with live fault injection: identical failure behavior, and literally zero route-classified outbound metrics in Prometheus, whether the `ServiceProfile` was present or deleted. The general lesson: verify a policy actually *engages* with real traffic, don't assume it does just because it deployed without error.

**Q: If you inherited this system and needed the mesh's traffic-management features to actually work for order-service's calls, what would you change, and what would that cost?**
The fix is architectural, not a mesh configuration change: order-service would need to resolve its downstream calls through Kubernetes Service DNS instead of Eureka's client-side discovery for those specific calls - either by switching those calls off Eureka entirely, or by running both in parallel during a migration. That's a genuine application-code and deployment-topology change to a decision made all the way back in Chapter 3, not a small tweak - which is exactly why this project documented it as a finding with an explicitly out-of-scope fix rather than attempting it inside the mesh sub-project that discovered it. It's a good real-world example of how an early architectural decision (client-side vs. server-side discovery) can have consequences that only surface many chapters later, once a different piece of infrastructure (the mesh) is layered on top with an implicit assumption the earlier decision doesn't satisfy.

**Q: What's a sidecar, and why is it an elegant solution specifically for cross-cutting network concerns?**
A sidecar is a second container in the same pod as the application container, sharing the pod's network namespace, so it can transparently intercept every byte the application sends and receives without the application needing to route through it explicitly or even be aware of it. It's elegant specifically for network-level cross-cutting concerns (encryption, retries, traffic shaping) because those concerns are naturally *about* the network boundary of the container, which is exactly the boundary the sidecar sits on - it wouldn't be nearly as natural a fit for, say, business-logic-level concerns (which need to understand the application's own domain, not just its network traffic).

**Q: How would you decide whether a given system actually needs a service mesh, versus handling resilience/security at the application layer?**
Consider the number of services and the cost of inconsistency: with a handful of services, application-level circuit breakers and manually configured TLS are manageable and give you fine-grained, business-logic-aware control. As the number of services and the number of teams owning them grows, the cost of ensuring every team correctly and consistently implements mTLS and retries grows too, and centralizing it at the infrastructure layer starts winning - but only if the mesh's actual attachment point (Service-addressed traffic) matches how your services actually discover each other, which is precisely the gap this project's own B3c finding exposes. A staff-level answer should include that last caveat explicitly: adopting a mesh without first confirming it can actually see your real traffic topology risks paying the mesh's operational cost for features that silently don't apply.

---

## Chapter 13 - Refactoring to microservices

### The book's idea

This chapter is about the opposite starting point from the rest of the book: you already have a working monolith, and want to migrate it to microservices *incrementally*, without a risky big-bang rewrite. This is the **strangler application** approach, via three levers, in order of increasing impact and increasing difficulty:

1. **Implement new features as standalone services.** The fastest, lowest-risk lever - it stops the monolith growing further and proves the microservices approach delivers value quickly, without touching any existing, working code. The book's own worked example is a Delayed Order Service, built new rather than added to the monolith.
2. **Split the presentation tier from the backend.** A partial win - you end up with two smaller monoliths, not real services yet, but it does expose a remote API boundary that future extracted services can call into, laying groundwork for the next lever.
3. **Extract existing business capabilities from the monolith, one at a time.** The lever that actually shrinks the monolith, and the hard one: splitting the domain model out means replacing internal object references with primary keys (Chapter 5's aggregate rules, applied retroactively), and often splitting apart a god class's mixed responsibilities (the book's own worked example: extracting a `Delivery` entity out of an `Order` god class).

Extracted pieces and the remaining monolith keep collaborating through **integration glue** (adapters in both the service and the monolith, using some IPC mechanism from Chapter 3), and that glue always needs an **anti-corruption layer** - a translation boundary preventing the monolith's often out-of-date or badly named domain model from leaking into the new service's clean one. This is exactly DDD's ACL pattern, applied at a migration boundary rather than a permanent service boundary.

The most non-obvious insight in the whole chapter: **the order you extract services in determines how much the monolith itself has to change.** A saga step still living inside the monolith only needs to become a compensatable transaction (with a semantic-lock state and rollback logic added to previously simple, ACID-transactional monolith code) if some *later* step in the saga can still fail after it. By extracting services in the right sequence - e.g. Order Service, then Consumer Service, then Kitchen Service, then Accounting Service, mirroring this project's own Chapter 4 saga step ordering - you can arrange for the monolith's own remaining step to always be the saga's *pivot* transaction, the point of no return, so the monolith never needs compensating-transaction logic added to it at all. Extraction order is a lever for minimizing risky changes to code you haven't migrated yet, not just a scheduling convenience.

Auth migration is solved with a surprisingly small change: the monolith's login handler adds one extra `USERINFO` cookie (a JWT) alongside its normal session cookie, and the API gateway maps that cookie to an `Authorization` header when calling any extracted service - letting session-based and token-based auth coexist without a big-bang security rewrite.

### Alternatives and why they lose

- **Big-bang rewrite** (freeze the monolith, build the whole new microservices system in parallel, cut over all at once). High risk (a long period with no ability to ship value, and a single enormous cutover where anything can go wrong), and the book's whole argument in this chapter is that this risk is almost never necessary - the strangler approach delivers value incrementally and de-risks each individual extraction.
- **Extracting services in whatever order is organizationally convenient, ignoring saga structure.** Can accidentally leave the monolith's remaining piece in the *middle* of a saga rather than at its pivot, forcing compensating-transaction logic to be retrofitted into code that was never designed for it - real, avoidable extra work that the right extraction order sidesteps entirely.

### A concrete example

The book's own worked example is extracting a `Delivery` entity out of an `Order` god-class inside the monolith - a single class doing too much (order details *and* delivery tracking *and* everything else) gets split into two, with the new Delivery Service reached from the monolith through an anti-corruption layer rather than the monolith reaching directly into the new service's internals (which would just re-couple them, defeating the extraction).

### What was actually built

Nothing in code - this project was built as microservices from Chapter 1 onward, so there's no monolith here to strangle. The chapter was read in full for its concepts rather than skipped, because two of its ideas are genuinely useful even without a monolith to migrate: the extraction-order-as-pivot-transaction-lever insight (directly reusable reasoning any time you're incrementally re-architecting a system with saga-like cross-component workflows, monolith or not), and the dual-mode auth trick (a monolith adds one extra JWT cookie alongside its session cookie; the gateway translates it to an `Authorization` header for extracted services) - conceptually the same problem this project's own `ftgo-authorization-server` (Chapter 11) solved by starting fresh with JWTs from day one, rather than needing to bridge from a legacy session-based mechanism.

### Interview prep

**Q: You've been asked to lead a migration of a legacy monolith to microservices. What's your actual first move, and why not just start extracting the most painful part first?**
Start with the lowest-risk lever: implement new features as standalone services, not touching the monolith's existing code at all - this proves the approach works operationally (deployment pipeline, monitoring, on-call for a new service) with zero risk to anything already working, and stops the monolith from getting any bigger while a longer-term extraction plan is built. "Extract the most painful part first" is tempting but often wrong as a *first* move specifically because the most painful part is usually also the most tangled (why else would it be painful) - better to build extraction muscle and infrastructure on a lower-risk capability before tackling the hardest one.

**Q: Explain why extraction order matters for saga compensation logic, with a concrete example.**
If the monolith's saga step is extracted *before* a later step that could still fail, the extracted service now needs full compensation logic (a semantic lock, a rollback path) it never needed as part of the monolith's single ACID transaction - real new complexity added specifically because of extraction order. If instead you extract services so the monolith's *remaining* step is always the saga's pivot transaction (the point after which nothing can fail and need compensating), the monolith never needs that logic added at all, because by the time its step runs, every step that could fail already has. Concretely: if Accounting is extracted first while Order and Kitchen remain in the monolith, and Accounting's credit check can still fail *after* the monolith's own Kitchen step already ran, the monolith's Kitchen step now needs a compensating "cancel the ticket" path it never needed before extraction - avoidable by extracting Accounting *after* the monolith's own remaining pivot step instead.

**Q: What's an anti-corruption layer, and why is it necessary even for a supposedly "temporary" migration boundary?**
It's a translation layer between two different domain models - here, the monolith's (often legacy, sometimes inconsistently named or outdated) model and the newly extracted service's clean one - so neither side's model leaks directly into the other's code. It's necessary even for something framed as temporary because "temporary" integration code has a well-known tendency to outlive its intended lifespan, and without a translation boundary, the new service's domain model gets silently polluted by the monolith's legacy concepts from day one, undermining the entire point of extracting a clean model in the first place. It's the same underlying pattern as DDD's ACL (Chapter 5's domain-modeling discipline, applied at an integration boundary rather than a permanent one).

**Q: How does the dual-mode session/JWT auth trick work, and where else does the same underlying idea show up?**
The monolith's login handler is changed minimally: alongside its normal session cookie, it also issues one extra `USERINFO` cookie containing a signed JWT with the same identity/claims. The API gateway, when routing a request to an already-extracted service, reads that JWT cookie and translates it into a standard `Authorization: Bearer <token>` header - so extracted services only ever need to understand JWT-based auth, never the monolith's session mechanism, while the monolith itself barely changes. The same underlying idea - bridge two different auth mechanisms at a single translation point instead of making every consumer understand both - is conceptually what this project's own `ftgo-authorization-server` and gateways do for OAuth2/JWT validation, just without a legacy session mechanism to actually bridge from, since there was never a monolith here.

**Q: A staff engineer on a legacy migration is asked "why is this taking so long, can't we just cut over faster?" How do you answer, grounded in this chapter's ideas?**
The strangler approach is deliberately incremental *because* the alternative (a big-bang rewrite) concentrates all the risk into one irreversible cutover moment, with a long preceding period of delivering no new value at all. The honest answer to "why so long" is that the migration's pace is set by how carefully each extraction's compensation/ACL/auth-bridging work needs to be done to keep the *existing* system working throughout - and that the specific ordering of extractions (per the pivot-transaction insight above) is itself chosen to minimize the amount of retrofit work needed in the monolith, which is often invisible from outside but is exactly what keeps each individual step low-risk. Going faster by skipping that ordering discipline trades a longer, safer migration for a shorter one with materially higher risk of a production incident in code that previously never needed compensating-transaction logic at all.

---

## Cross-cutting staff-level questions

These span multiple chapters and are the kind of question a staff+ panel asks to see whether you can reason across pattern boundaries, not just recite one pattern at a time.

**Q: Trace one request - "customer revises an order" - through this system, naming every pattern from this document it touches.**
The request hits `ftgo-public-gateway` (Ch.8, API gateway), which validates the caller's JWT (Ch.11, security) and routes to `order-service`. `order-service`'s controller invokes `Order.revise()` on the aggregate (Ch.5, DDD aggregate - the state transition is enforced on the entity itself), which returns domain events written to the outbox in the same transaction as the state change (Ch.3, transactional outbox) - or, in event-sourced mode, appended to the event store with optimistic concurrency control (Ch.6). This kicks off the Revise Order saga (Ch.4) - in orchestration mode, `ReviseOrderSagaOrchestrator` issues commands to accounting-service to adjust the credit reservation; if declined, a compensating action reverts the line items. Every hop is traced end-to-end with a shared `traceId` (Ch.11, distributed tracing) and, in the Kubernetes deployment, encrypted in transit by the Linkerd sidecar (Ch.12) - though notably, order-service's own downstream calls in this flow go through Eureka, not Kubernetes Service DNS, so the mesh's retry policies don't apply to them (Ch.12's own documented finding). The whole interaction is also captured by the audit-logging aspect (Ch.11) regardless of whether the revision is ultimately approved or declined, and downstream, `ftgo-order-history-service`'s CQRS read model (Ch.7) picks up the resulting events and eventually reflects the new state for fast reads.

**Q: Name a decision in this system that made an earlier chapter's implementation simpler at the cost of a later chapter's implementation being harder - and how you'd decide, as a staff engineer, whether that trade was worth it.**
Client-side service discovery via Eureka (Ch.3) is simple, well-understood, and gets every service talking to every other service quickly and correctly, with fine-grained load-balancing control in application code. It's also the direct cause of Ch.12's mesh-traffic-management finding: `ServiceProfile`-based retries configured for order-service's downstream calls have no effect, because Eureka resolves calls straight to pod IPs, bypassing the Kubernetes Service identity the mesh's policies are keyed on. Deciding whether that trade was worth it isn't answerable in the abstract - it depends on whether, at the time Chapter 3's decision was made, a service mesh was even a planned part of the architecture. If it wasn't yet in scope, Eureka was a perfectly reasonable choice for the problem being solved at the time, and the mesh's later gap is a genuine, but not obviously avoidable, cost of sequencing - which is itself a useful staff-level point: not every cross-chapter friction is a mistake, some are the ordinary cost of a system evolving, and the real skill is recognizing the friction, documenting it clearly (as this project did, rather than silently shipping a mesh policy that quietly doesn't work), and making a deliberate call about whether to pay the cost of reconciling it now or later.

**Q: This system implements the same underlying idea (compose data from multiple sources) in three different places - Ch.7's API composition, Ch.7's CQRS, and Ch.8's gateway-level composition. Why isn't that duplication a code smell?**
Because they solve the composition problem at three genuinely different points with three genuinely different trade-offs, not three redundant implementations of the same trade-off. Ch.7's API composition composes live, inside a service, when freshness matters most and the composed calls are few. Ch.7's CQRS composes ahead of time into a durable read model, when read volume or read-side availability decoupling matters most. Ch.8's gateway-level composition composes at the edge, specifically for one client type's convenience (fewer round trips for a mobile client), independent of whether the services being composed also expose their own composed views for other purposes. A staff engineer should recognize this as the same underlying *pattern* (assemble one view from many sources) applied deliberately at different *layers* for different *reasons* - genuine architectural pluralism, not accidental duplication - and be able to articulate, for a new composition need, which of the three layers it actually belongs at.

**Q: If you had to remove exactly one pattern from this entire system to reduce operational complexity, with the least loss of the properties this project cares about, which would you cut and why?**
A defensible answer: event sourcing for the `Order` aggregate (Ch.6) specifically, kept as the JPA persistence mode's sibling rather than the only option. It was deliberately built for the learning exercise despite `Order`'s short lifecycle not strictly needing full history/replayability, and this project's own documentation is explicit that it added real complexity (snapshot mechanism, dedicated version tracking, the pseudo-event machinery for orchestration-mode sagas) beyond what `Order`'s actual access pattern demands. Removing it would lose full audit-trail-style history for Order specifically, but that's substantially covered already by the separate, purpose-built audit-logging system (Ch.11) for "who did what," and by the CQRS read model (Ch.7) for "what does this order currently look like" - so the marginal, unique value event sourcing adds specifically for `Order` is smaller than for aggregates where replay-driven read models or true point-in-time reconstruction are actual product requirements, not primarily learning goals. A weaker answer would be to cut something structurally load-bearing (like the transactional outbox, which nearly everything else depends on for correctness) without acknowledging that doing so would remove a *correctness* guarantee, not just an operational nicety - the interviewer is listening for whether you distinguish "this added complexity for a reason that's now optional" from "this is complexity that's actually doing necessary work."

---

## Where to go next

- **`docs/ARCHITECTURE.md`** - the technical reference: sequence diagrams for every saga (both styles), the Kafka topic catalog, and a deep dive into every pattern summarized above.
- **`docs/CH12-DEPLOYMENT.md`** - the container/Compose deployment details for Chapter 12's first sub-project.
- **`README.md`** - what's running today: the full service list, ports, and how to bring the stack up locally.
- **`CONTEXT.md`** - a working learner's notebook: session-by-session log of what was built when, plus a running "Understood well" / "Needs more depth" account of the book's concepts.
