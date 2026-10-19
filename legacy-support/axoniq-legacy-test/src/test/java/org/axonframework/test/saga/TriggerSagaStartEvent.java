/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
