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

import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * Command payload dedicated to triggering the resolution of parameters through a custom {@link
 * ParameterResolverFactory}.
 *
 * @author Steven van Beelen
 */
public class ResolveParameterCommand {

    @TargetAggregateIdentifier
    private final Object aggregateIdentifier;

    ResolveParameterCommand(Object aggregateIdentifier) {
        this.aggregateIdentifier = aggregateIdentifier;
    }

    public Object getAggregateIdentifier() {
        return aggregateIdentifier;
    }
}
