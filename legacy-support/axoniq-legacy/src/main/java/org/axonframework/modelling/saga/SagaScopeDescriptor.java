/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.messaging.ScopeDescriptor;

import java.beans.ConstructorProperties;
import java.io.Serial;
import java.util.Objects;

/**
 * Describes the scope of a Saga by means of its type and identifier.
 *
 * @author Steven van Beelen
 * @since 3.3
 */
public class SagaScopeDescriptor implements ScopeDescriptor {

    @Serial
    private static final long serialVersionUID = 4162755498638204691L;

    private final String type;
    private final Object identifier;

    /**
     * Instantiate a SagaScopeDescriptor with the provided {@code type} and {@code identifier}.
     *
     * @param type       A {@link String} describing the type of the Saga
     * @param identifier An {@link Object} denoting the identifier of the Saga
     */
    @JsonCreator
    @ConstructorProperties({ "type", "identifier" })
    public SagaScopeDescriptor(@JsonProperty("type") String type, @JsonProperty("identifier") Object identifier) {
        this.type = type;
        this.identifier = identifier;
    }

    /**
     * The type of Saga described by this scope
     *
     * @return the type of Saga described by this scope
     */
    public String getType() {
        return type;
    }

    /**
     * The identifier of the Saga targeted with this scope
     *
     * @return identifier of the Saga targeted with this scope
     */
    public Object getIdentifier() {
        return identifier;
    }

    @Override
    public String scopeDescription() {
        return String.format("SagaScopeDescriptor for type [%s] and identifier [%s]", type, identifier);
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
        final SagaScopeDescriptor other = (SagaScopeDescriptor) obj;
        return Objects.equals(this.type, other.type)
                && Objects.equals(this.identifier, other.identifier);
    }

    @Override
    public String toString() {
        return "SagaScopeDescriptor{" +
                "type='" + type + '\'' +
                ", identifier=" + identifier +
                '}';
    }
}
