/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import io.axoniq.framework.workflow.runtime.association.EqualsComparison;
import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.association.MetadataPropertyValueRetriever;
import io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever;
import io.axoniq.framework.workflow.runtime.association.ValueRetriever;

/**
 * DSL-level helpers for authoring serialized event associations.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public final class EventAssociationsUtils {

    private EventAssociationsUtils() {
        // utility class
    }

    /**
     * Creates a payload-property association source.
     *
     * @param propertyName payload property name
     * @return payload-property retriever
     */
    public static ValueRetriever payloadProperty(String propertyName) {
        return PayloadPropertyValueRetriever.payloadProperty(propertyName);
    }

    /**
     * Creates a metadata-property association source.
     *
     * @param propertyName metadata key
     * @return metadata-property retriever
     */
    public static ValueRetriever metadataProperty(String propertyName) {
        return MetadataPropertyValueRetriever.metadataProperty(propertyName);
    }

    /**
     * Creates an equals matcher for the association DSL.
     *
     * @param value expected association value
     * @return equals matcher
     */
    public static Associations.Matcher equalsTo(Object value) {
        return new Associations.Matcher(EqualsComparison.OPERATOR, value);
    }

    /**
     * Short alias for {@link #equalsTo(Object)}.
     *
     * @param value expected association value
     * @return equals matcher
     */
    public static Associations.Matcher eq(Object value) {
        return equalsTo(value);
    }
}
