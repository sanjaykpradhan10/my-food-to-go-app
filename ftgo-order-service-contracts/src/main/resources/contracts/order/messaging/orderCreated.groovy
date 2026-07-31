package contracts.order.messaging

import org.springframework.cloud.contract.spec.Contract

Contract.make {
    label 'orderCreated'
    input {
        triggeredBy('orderCreated()')
    }
    outputMessage {
        sentTo('order.events')
        body(
                eventId: $(consumer(regex('[0-9a-f-]{36}')), producer('11111111-1111-1111-1111-111111111111')),
                eventType: 'OrderCreated',
                orderId: 1223232,
                consumerId: 1,
                restaurantId: 1,
                lineItems: [
                        [menuItemId: 10, quantity: 2]
                ]
        )
    }
}
