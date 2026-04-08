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

package org.axonframework.modelling.command;


import org.axonframework.common.util.StubDomainEvent;

import java.util.UUID;

/**
 * Sample aggregate used by, for example, the {@link LockingRepositoryTest}.
 *
 * @author Allard Buijze
 */
public class StubAggregate {

    @SuppressWarnings("FieldMayBeFinal")
    @AggregateIdentifier
    private String identifier;

    public StubAggregate() {
        identifier = UUID.randomUUID().toString();
    }

    public StubAggregate(Object identifier) {
        this.identifier = identifier != null ? identifier.toString() : null;
    }

    public void doSomething() {
        AggregateLifecycle.apply(new StubDomainEvent());
    }

    public String getIdentifier() {
        return identifier;
    }

    public void delete() {
        AggregateLifecycle.apply(new StubDomainEvent());
        AggregateLifecycle.markDeleted();
    }
}
