/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation;

import org.axonframework.common.annotation.Internal;

import java.util.Collection;
import java.util.Map;

/**
 * Classifies whether a runtime class is a structural (carrier) representation rather than a
 * domain payload. Structural classes describe shape, not identity: {@code byte[]},
 * {@code Map}, {@code List}, {@code String}, {@code Number}, and tree-node types from JSON
 * libraries (Jackson 2 {@code com.fasterxml.jackson.databind.JsonNode}, Jackson 3
 * {@code tools.jackson.databind.JsonNode}).
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class StructuralPayloadTypes {

    private static final String JACKSON_2_TREE_NODE_FQN = "com.fasterxml.jackson.databind.JsonNode";
    private static final String JACKSON_3_TREE_NODE_FQN = "tools.jackson.databind.JsonNode";

    private StructuralPayloadTypes() {
    }

    /**
     * Returns {@code true} when {@code payloadClass} is a structural carrier type whose runtime class never
     * identifies a domain payload.
     *
     * @param payloadClass the runtime class to classify
     * @return {@code true} when the class is structural, {@code false} when it could carry domain identity
     */
    static boolean isStructural(Class<?> payloadClass) {
        if (payloadClass.isArray()) {
            return true;
        }
        if (Map.class.isAssignableFrom(payloadClass)) {
            return true;
        }
        if (Collection.class.isAssignableFrom(payloadClass)) {
            return true;
        }
        if (CharSequence.class.isAssignableFrom(payloadClass)) {
            return true;
        }
        if (Number.class.isAssignableFrom(payloadClass)) {
            return true;
        }
        return isJacksonTreeNode(payloadClass);
    }

    /**
     * Returns {@code true} when {@code payloadClass} or any superclass is a Jackson tree-node type (Jackson 2 or 3).
     */
    private static boolean isJacksonTreeNode(Class<?> payloadClass) {
        Class<?> current = payloadClass;
        while (current != null && current != Object.class) {
            String fullyQualifiedName = current.getName();
            if (JACKSON_2_TREE_NODE_FQN.equals(fullyQualifiedName)
                    || JACKSON_3_TREE_NODE_FQN.equals(fullyQualifiedName)) {
                return true;
            }
            current = current.getSuperclass();
        }
        return false;
    }
}
