package contracts.kitchen

import org.springframework.cloud.contract.spec.Contract

Contract.make {
    label 'shouldCreateTicket'
    input {
        // messageFrom/messageBody don't exist on spring-cloud-contract-spec-java 4.3.4's Input
        // class (verified via javap - the brief's draft targeted an API this project doesn't
        // have). triggeredBy() alone is sufficient here, matching orderCreated.groovy's pattern:
        // MessagingBase.createTicketCommandReceived() hardcodes the CreateTicket command payload
        // itself and calls KitchenCommandListener.onMessage() directly, so there's no need for
        // the DSL to separately describe an inbound message shape.
        triggeredBy('createTicketCommandReceived()')
    }
    outputMessage {
        sentTo('saga.replies')
        body(
                // Unlike orderCreated.groovy's fixed test eventId (order-service's MessagingBase
                // passes a hardcoded UUID into the publish call it drives), TicketService.publishReply
                // generates the reply's eventId internally via UUID.randomUUID() - this provider test
                // has no way to pin it to a literal. producer(regex(...)) rather than a fixed
                // producer(...) string lets the provider-side generated assertion accept any UUID,
                // matching what the code actually produces.
                eventId: $(consumer(regex('[0-9a-f-]{36}')), producer(regex('[0-9a-f-]{36}'))),
                participant: 'kitchen',
                eventType: 'TicketCreated',
                orderId: 1223232,
                reason: null,
                sagaType: 'CreateOrder'
        )
    }
}
