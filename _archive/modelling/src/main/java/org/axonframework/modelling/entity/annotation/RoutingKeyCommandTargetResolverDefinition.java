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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.modelling.entity.child.CommandTargetResolver;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Member;

import static java.lang.String.format;
import static org.axonframework.common.ReflectionUtils.getMemberValueType;

/**
 * Definition for creating {@link CommandTargetResolver} instances based on the
 * {@link EntityMember#routingKey routing key attribute}.
 * <p>
 * The routing key of <b>both</b> the message and entity is determined by the {@link EntityMember#routingKey attribute}
 * on the declaring member in the parent entity. The routing key of the message and of the entity are matched to
 * determine if a child entity should handle a given message.
 *
 * @author Mitchell Herrijgers
 * @see RoutingKeyEventTargetMatcher
 * @since 5.0.0
 */
public class RoutingKeyCommandTargetResolverDefinition implements CommandTargetResolverDefinition {

    @Override
    public <E> CommandTargetResolver<E> createCommandTargetResolver(
            AnnotatedEntityMetamodel<E> entity,
            Member member
    ) {
        String routingKey = RoutingKeyUtils.getMessageRoutingKey((AnnotatedElement) member).orElse(null);
        if (routingKey != null) {
            return new RoutingKeyCommandTargetResolver<>(entity, routingKey, routingKey);
        }

        // No routing key found, perhaps we are dealing with a single occurrence entity member.
        Class<?> memberValueType = getMemberValueType(member);
        if (Iterable.class.isAssignableFrom(memberValueType)) {
            throw new AxonConfigurationException(
                    format("Member [%s] of type [%s] is a collection type, but the child does not define a routing key. "
                                   + "Please set the \"routingKey\" property on the @EntityMember annotation "
                                   + "or implement a custom CommandTargetResolver for this collection type.",
                           member, memberValueType)
            );
        }
        // If the member is not a collection type, we can assume it is a single entity.
        // This does not require an explicit @RoutingKey, as there might only be one in the aggregate.
        // If the user has multiple single-entity child entities in the same parent, commands will lead to a ChildAmbiguityException, which is clear enough.
        return CommandTargetResolver.MATCH_ANY();
    }
}
