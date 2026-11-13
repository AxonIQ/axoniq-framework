/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.messaging.ScopeDescriptor;
import org.jspecify.annotations.NonNull;


import java.util.Objects;

public class TestScopeDescriptor implements ScopeDescriptor {

    private final String type;
    private Object identifier;

    @JsonCreator
    public TestScopeDescriptor(@JsonProperty("type") String type, @JsonProperty("identifier") Object identifier) {
        this.type = type;
        this.identifier = identifier;
    }

    public String getType() {
        return type;
    }

    public Object getIdentifier() {
        return identifier;
    }

    @Override
    public @NonNull String scopeDescription() {
        return String.format("TestScopeDescriptor for type [%s] and identifier [%s]", type, identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, identifier);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TestScopeDescriptor other = (TestScopeDescriptor) obj;
        return Objects.equals(this.type, other.type)
                && Objects.equals(this.identifier, other.identifier);
    }

    @Override
    public String toString() {
        return "TestScopeDescriptor{" +
                "type=" + type +
                ", identifier='" + identifier + '\'' +
                '}';
    }
}
