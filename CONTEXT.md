# FTGO Study Context — Microservices Patterns (Chris Richardson)

## About this file
This file is the shared brain between Claude Chat (claude.ai) and Claude Code.
- **In Claude Chat**: paste this file at the start of a session to restore context
- **In Claude Code**: this file lives in the project root — Claude Code reads it automatically

Update this file at the end of every session (either tool can do it).

---

## Learner profile
- **Name**: Sanjay
- **Goal**: Deep, job-ready understanding of microservices patterns — not just interview prep
- **Background**: Senior software engineer, Java/Spring Boot experience
- **Approach**: Read the entire book chapter by chapter, building the FTGO app alongside it
- **Reference implementation**: https://github.com/microservices-patterns/ftgo-application
- **Local project**: ~/Sanjay/Projects/Spring/my-food-to-go-app

---

## Book structure & progress

| Ch | Title | Status | Confidence | Notes |
|----|-------|--------|------------|-------|
| 1  | Escaping monolithic hell | Done | High | Hexagonal arch, scale cube (X/Y/Z), monolithic hell symptoms, pattern language structure |
| 2  | Decomposition strategies | Done | High | 4+1 views, hexagonal arch, system ops, business capability, DDD subdomains, SRP/CCP, god classes, bounded contexts |
| 3  | Interprocess communication in a microservice architecture | Not started | — | |
| 4  | Managing transactions with sagas | Not started | — | |
| 5  | Designing business logic in a microservice architecture | Not started | — | |
| 6  | Developing business logic with event sourcing | Not started | — | |
| 7  | Implementing queries in a microservice architecture | Not started | — | |
| 8  | External API patterns | Not started | — | |
| 9  | Testing microservices: Part 1 | Not started | — | |
| 10 | Testing microservices: Part 2 | Not started | — | |
| 11 | Developing production-ready services | Not started | — | |
| 12 | Deploying microservices | Not started | — | |
| 13 | Refactoring to microservices | Not started | — | |

**Status options**: Not started → Reading → Implementing → Review → Done
**Confidence**: Low / Medium / High

---

## Current position

- **Chapter**: 3 — Interprocess communication in a microservice architecture
- **Status**: Not started
- **Last session**: 2026-06-18
- **Last tool used**: Claude Chat

---

## Session log
<!-- Add a one-liner after each session: date · tool · what was covered -->
- 2026-06-18 · Claude Chat · Ch. 1 complete — monolithic hell, scale cube, benefits/drawbacks, pattern language overview
- 2026-06-18 · Claude Chat · Ch. 2 complete — 4+1 view model, hexagonal architecture, system operations, decompose by business capability, decompose by DDD subdomain, SRP/CCP, god classes resolved via bounded contexts

---

## Concept understanding

### Understood well
- Ch. 1: Hexagonal architecture, monolithic hell (6 symptoms), scale cube (X/Y/Z axes), microservices as Y-axis decomposition, database-per-service rationale, pattern language structure (forces / resulting context / related patterns)
- Ch. 2: 4+1 view model (logical/implementation/process/deployment/scenarios), hexagonal architecture (inbound/outbound adapters and ports), three-step architecture process (system ops → services → APIs), decompose by business capability, decompose by DDD subdomain, SRP and CCP applied to services, god class problem and resolution via per-service domain models, bounded context = service boundary, Ubiquitous Language per service (Order vs Ticket vs Delivery)

### Needs more depth
- (none yet)

### Open questions
- (none yet)

---

## FTGO app build log

### Project setup
- [ ] Clone reference implementation for reference: `git clone https://github.com/microservices-patterns/ftgo-application`
- [ ] Initialise my-food-to-go-app project structure
- [ ] Set up Docker Compose for local infrastructure (MySQL, Kafka, Zookeeper)
- [ ] Verify local environment runs

### Services to build (one per chapter as relevant)
| Service | Introduced | Status | Notes |
|---------|-----------|--------|-------|
| ftgo-consumer-service | Ch. 1–2 | Ready to scaffold | Identified in capability mapping |
| ftgo-order-service | Ch. 2–4 | Ready to scaffold | Core saga orchestrator; owns Order domain model |
| ftgo-kitchen-service | Ch. 2, 5 | Ready to scaffold | Uses Ticket not Order; separate bounded context |
| ftgo-accounting-service | Ch. 2, 4 | Ready to scaffold | |
| ftgo-restaurant-service | Ch. 2 | Ready to scaffold | |
| ftgo-delivery-service | Ch. 2 | Ready to scaffold | Uses Delivery not Order; separate bounded context |
| ftgo-api-gateway | Ch. 8 | Not started | |

### Architecture decisions made
- (none yet — will populate from Ch. 2 onwards)

### Code patterns implemented
- (none yet)

---

## Patterns reference
<!-- Tick off as each pattern is understood AND implemented -->

### Decomposition
- [x] Decompose by business capability (Ch. 2)
- [x] Decompose by subdomain (Ch. 2)

### Communication
- [ ] Remote procedure invocation / REST (Ch. 3)
- [ ] Messaging (Ch. 3)
- [ ] Circuit breaker (Ch. 3)
- [ ] Client-side discovery / Server-side discovery (Ch. 3)
- [ ] Transactional outbox (Ch. 3)
- [ ] Transaction log tailing (Ch. 3)

### Data consistency
- [ ] Saga — choreography (Ch. 4)
- [ ] Saga — orchestration (Ch. 4)

### Business logic
- [ ] Domain model / DDD aggregates (Ch. 5)
- [ ] Domain events (Ch. 5)
- [ ] Transaction script (Ch. 5)
- [ ] Event sourcing (Ch. 6)

### Querying
- [ ] API composition (Ch. 7)
- [ ] CQRS (Ch. 7)

### External API
- [ ] API gateway (Ch. 8)
- [ ] Backends for frontends (Ch. 8)

### Testing
- [ ] Consumer-driven contract test (Ch. 9)
- [ ] Component test (Ch. 10)
- [ ] Service component test (Ch. 10)

### Observability & operations
- [ ] Health check API (Ch. 11)
- [ ] Log aggregation (Ch. 11)
- [ ] Distributed tracing (Ch. 11)
- [ ] Externalized configuration (Ch. 11)

### Deployment
- [ ] Deploy as container (Ch. 12)
- [ ] Service mesh (Ch. 12)
- [ ] Sidecar (Ch. 12)

### Refactoring
- [ ] Strangler application (Ch. 13)
- [ ] Anti-corruption layer (Ch. 13)

---

## How to use this file

### Starting a session in Claude Chat (claude.ai)
Paste the following prompt:
> "I'm working through Microservices Patterns by Chris Richardson, building the FTGO app as I go.
> Here is my current CONTEXT.md: [paste file]. Please resume from where I left off."

### Starting a session in Claude Code
Claude Code reads this file from the project root automatically. You can also say:
> "Read CONTEXT.md and resume my FTGO study session. I want to work on [concept / service / chapter]."

### Ending any session
Ask whichever tool you're in:
> "Update CONTEXT.md to reflect what we covered today."

---

## Tech stack decisions
- **Language**: Java 17+
- **Framework**: Spring Boot 3.x
- **Messaging**: Apache Kafka (via Eventuate Tram)
- **Database**: MySQL per service (each service owns its schema)
- **Infrastructure**: Docker Compose (local), Kubernetes deferred to Ch. 12
- **Build tool**: Gradle (matches reference implementation)
- **Testing**: JUnit 5, Mockito, Spring Boot Test, Pact (contract tests in Ch. 9)

---
*Last updated: 2026-06-04 — initial setup*
