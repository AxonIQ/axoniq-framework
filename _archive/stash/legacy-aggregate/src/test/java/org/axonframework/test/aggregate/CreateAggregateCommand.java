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
 * Sample command message to construct the {@link AnnotatedAggregate}.
 *
 * @author Allard Buijze
 */
class CreateAggregateCommand {

    public static final boolean SHOULD_NOT_PUBLISH = false;
    public static final boolean SHOULD_PUBLISH = true;

    private final Object aggregateIdentifier;
    private final boolean shouldPublishEvents;

    public CreateAggregateCommand() {
        this(null);
    }

    public CreateAggregateCommand(Object aggregateIdentifier) {
        this(aggregateIdentifier, SHOULD_PUBLISH);
    }

    public CreateAggregateCommand(Object aggregateIdentifier, boolean shouldPublishEvents) {
        this.aggregateIdentifier = aggregateIdentifier;
        this.shouldPublishEvents = shouldPublishEvents;
    }

    public Object getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    public boolean shouldPublishEvents() {
        return shouldPublishEvents;
    }
}
