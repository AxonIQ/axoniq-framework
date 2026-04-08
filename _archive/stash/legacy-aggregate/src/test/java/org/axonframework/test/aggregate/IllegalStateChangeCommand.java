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

package org.axonframework.test.aggregate;

/**
 * @author Allard Buijze
 */
public class IllegalStateChangeCommand {

    private final Object aggregateIdentifier;
    private final Integer newIllegalValue;

    public IllegalStateChangeCommand(Object aggregateIdentifier, Integer newIllegalValue) {
        this.aggregateIdentifier = aggregateIdentifier;
        this.newIllegalValue = newIllegalValue;
    }

    public Object getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    public Integer getNewIllegalValue() {
        return newIllegalValue;
    }
}
