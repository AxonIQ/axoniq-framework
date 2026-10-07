/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.modelling.command;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.messaging.ScopeDescriptor;
import org.jspecify.annotations.Nullable;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.util.Objects;
import java.util.function.Supplier;

import static org.axonframework.common.Assert.notNull;

/**
 * Describes the scope of an Aggregate by means of its type and identifier.
 *
 * @author Steven van Beelen
 * @since 3.3
 */
public class AggregateScopeDescriptor implements ScopeDescriptor {

    @Serial
    private static final long serialVersionUID = 3584695571254668002L;

    private final String type;
    private @Nullable Object identifier;
    private transient @Nullable Supplier<Object> identifierSupplier;

    /**
     * Instantiate an AggregateScopeDescriptor with a {@code type} and {@code identifierSupplier}. Using the {@code
     * identifierSupplier} instead of an {@link Object} for the {@code identifier} allows the creating processes to
     * provide the identifier lazily.
     * This is necessary when Aggregate's identifier is not created yet, for example when the AggregateScopeDescriptor
     * is created whilst the Aggregate is still under construction.
     *
     * @param type               a {@link String} describing the type of the Aggregate
     * @param identifierSupplier a {@link Supplier} of {@link Object}, which can supply the identifier of the Aggregate
     */
    public AggregateScopeDescriptor(String type, Supplier<Object> identifierSupplier) {
        notNull(
                identifierSupplier,
                () -> "A Supplier for the identifier field is required when using this constructor"
        );

        this.type = type;
        this.identifierSupplier = identifierSupplier;
    }

    /**
     * Instantiate an AggregateScopeDescriptor with the provided {@code type} and {@code identifier}.
     *
     * @param type       a {@link String} describing the type of the Aggregate
     * @param identifier an {@link Object} denoting the identifier of the Aggregate
     */
    @JsonCreator
    @ConstructorProperties({"type", "identifier"})
    public AggregateScopeDescriptor(@JsonProperty("type") String type, @JsonProperty("identifier") Object identifier) {
        Objects.requireNonNull(identifier, "The identifier may not be null");
        this.type = type;
        this.identifier = identifier;
    }

    /**
     * Returns the type of Aggregate, as String, targeted by this scope.
     *
     * @return the Aggregate targeted by this scope
     */
    public String getType() {
        return type;
    }

    /**
     * The identifier of the Aggregate targeted with this scope.
     *
     * @return the identifier of the target Aggregate
     */
    public Object getIdentifier() {
        if (identifier == null) {
            identifier = identifierSupplier.get();
        }
        return identifier;
    }

    /**
     * Resolves a lazily supplied identifier before the default serialization writes the fields, as the
     * {@link Supplier} itself is not serialized.
     */
    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        getIdentifier();
        out.defaultWriteObject();
    }

    @Override
    public String scopeDescription() {
        return String.format("AggregateScopeDescriptor for type [%s] and identifier [%s]", type, getIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, getIdentifier());
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final AggregateScopeDescriptor other = (AggregateScopeDescriptor) obj;
        return Objects.equals(this.type, other.type)
                && Objects.equals(this.getIdentifier(), other.getIdentifier());
    }

    @Override
    public String toString() {
        return "AggregateScopeDescriptor{" +
                "type=" + type +
                ", identifier='" + getIdentifier() + '\'' +
                '}';
    }
}
