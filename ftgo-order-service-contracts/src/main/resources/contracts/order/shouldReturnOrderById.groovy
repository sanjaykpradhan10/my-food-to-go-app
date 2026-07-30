package contracts.order

import org.springframework.cloud.contract.spec.Contract

Contract.make {
    description "should return an existing order by id"
    request {
        method GET()
        url '/orders/1223232'
    }
    response {
        status 200
        headers {
            header('Content-Type', 'application/json')
        }
        body(
                id: 1223232,
                consumerId: 1,
                restaurantId: 1,
                lineItems: [
                        [menuItemId: 10, quantity: 2]
                ],
                status: "APPROVAL_PENDING"
        )
    }
}
