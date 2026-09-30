/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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
