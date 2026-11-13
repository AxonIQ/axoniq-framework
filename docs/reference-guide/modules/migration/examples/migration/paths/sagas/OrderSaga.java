/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package migration.paths.sagas;

import org.axonframework.messaging.core.annotation.Namespace;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.StartSaga;
import org.axonframework.spring.stereotype.Saga;

// tag::namespace-preserves-token[]
// Axon Framework 4: @Saga @ProcessingGroup("orders")
@Saga
@Namespace("orders")
public class OrderSaga {

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    public void on(OrderPlaced event) {
        // The processor stays named "orders", so its Axon Framework 4 token is found and resumed.
    }
}
// end::namespace-preserves-token[]

record OrderPlaced(String orderId) {

}
