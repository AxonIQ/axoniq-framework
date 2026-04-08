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

package org.axonframework.deadline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.messaging.core.ScopeDescriptor;
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
