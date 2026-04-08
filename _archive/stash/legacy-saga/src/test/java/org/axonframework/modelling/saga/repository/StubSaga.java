/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.modelling.saga.repository;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.SagaLifecycle;

import static org.axonframework.modelling.saga.SagaLifecycle.associateWith;
import static org.axonframework.modelling.saga.SagaLifecycle.removeAssociationWith;

/**
* @author Allard Buijze
*/
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class StubSaga {

    public void registerAssociationValue(AssociationValue associationValue) {
        associateWith(associationValue);
    }

    public void removeAssociationValue(String key, String value) {
        removeAssociationWith(key, value);
    }

    public void end() {
        SagaLifecycle.end();
    }

    public void associate(String key, String value) {
        associateWith(key, value);
    }
}
