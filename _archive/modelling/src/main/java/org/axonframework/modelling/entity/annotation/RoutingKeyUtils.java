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

package org.axonframework.modelling.entity.annotation;

import org.axonframework.common.annotation.Internal;

import java.lang.reflect.AnnotatedElement;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.axonframework.common.annotation.AnnotationUtils.findAnnotationAttributes;

/**
 * Utility class for retrieving routing keys from entity members and child entities.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Internal
public class RoutingKeyUtils {

    private RoutingKeyUtils() {
        // Utility class, prevent instantiation
    }

    /**
     * Retrieves the routing key for the given member, which is defined by the {@link EntityMember#routingKey}
     * annotation.
     *
     * @param member The member to retrieve the routing key for.
     * @return An {@link Optional} containing the routing key if present, otherwise empty.
     */
    public static Optional<String> getMessageRoutingKey(AnnotatedElement member) {
        Objects.requireNonNull(member, "The member must not be null.");
        Optional<Map<String, Object>> attributes = findAnnotationAttributes(member, EntityMember.class);
        if (attributes.isEmpty()) {
            return Optional.empty();
        }
        String routingKeyProperty = (String) attributes.get().get("routingKey");
        if (!routingKeyProperty.isEmpty()) {
            return Optional.of(routingKeyProperty);
        }
        return Optional.empty();
    }
}
