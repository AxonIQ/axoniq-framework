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

package org.axonframework.test.saga;

import java.util.Objects;

/**
 * Event signaling the start of a saga.
 *
 * @author Allard Buijze
 */
public class TriggerSagaStartEvent {

    private final String identifier;
    private final String deadlineName;

    public TriggerSagaStartEvent(String identifier) {
        this(identifier, "deadlineName");
    }

    public TriggerSagaStartEvent(String identifier, String deadlineName) {
        this.identifier = identifier;
        this.deadlineName = deadlineName;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getDeadlineName() {
        return deadlineName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TriggerSagaStartEvent that = (TriggerSagaStartEvent) o;
        return Objects.equals(identifier, that.identifier)
                && Objects.equals(deadlineName, that.deadlineName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, deadlineName);
    }

    @Override
    public String toString() {
        return "TriggerSagaStartEvent{" +
                "identifier='" + identifier + '\'' +
                ", deadlineName='" + deadlineName + '\'' +
                '}';
    }
}
