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

import org.axonframework.modelling.entity.child.CommandTargetResolver;

import java.lang.reflect.Member;

/**
 * Defines how a {@link CommandTargetResolver} should be constructed for an {@link EntityMember}-annotated member of an
 * {@link AnnotatedEntityMetamodel}.
 *
 * @author Mitchell Herrijgers
 * @see AnnotatedEntityMetamodel
 * @see CommandTargetResolver
 * @see EntityMember
 * @since 5.0.0
 */
@FunctionalInterface
public interface CommandTargetResolverDefinition {

    /**
     * Creates a {@link CommandTargetResolver} for the given {@code entity} and {@code member}.
     *
     * @param metamodel The {@link AnnotatedEntityMetamodel} of the child entity.
     * @param member    The member that represents the child entity in the parent entity metamodel. This member is
     *                  typically a field or a method that returns the child entity, annotated with
     *                  {@link EntityMember}.
     * @param <E>       The type of the child entity.
     * @return A {@link CommandTargetResolver} that can be used to match child entities against messages.
     */
    <E> CommandTargetResolver<E> createCommandTargetResolver(
            AnnotatedEntityMetamodel<E> metamodel,
            Member member
    );
}
