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

package org.axonframework.modelling.command.inspection;

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.modelling.command.AggregateMember;
import org.axonframework.modelling.command.ForwardingMode;

import java.lang.reflect.Member;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Implementation of a {@link ChildEntityDefinition} that is used to detect single entities annotated with {@link
 * AggregateMember}. If such a field or method is found a {@link ChildEntity} is created that delegates to the entity.
 *
 * @author Allard Buijze
 * @since 3.0
 */
public class AggregateMemberAnnotatedChildEntityDefinition extends AbstractChildEntityDefinition {

    @Override
    protected boolean isMemberTypeSupported(Member member) {
        try {
            Class<?> valueType = ReflectionUtils.getMemberValueType(member);
            return !Iterable.class.isAssignableFrom(valueType) && !Map.class.isAssignableFrom(valueType);
        } catch (IllegalStateException e) {
            return false;
        }
    }

    @Override
    protected <T> EntityModel<Object> extractChildEntityModel(EntityModel<T> declaringEntity,
                                                              Map<String, Object> attributes,
                                                              Member member) {
        Class<?> entityClass = ReflectionUtils.getMemberValueType(member);
        if (entityClass.isInterface()) {
            throw new AxonConfigurationException(
                    "Aggregate Member type should be a concrete implementation instead of [" + entityClass + "]."
            );
        }

        @SuppressWarnings("unchecked")
        Class<Object> castEntityClass = (Class<Object>)entityClass;

        return declaringEntity.modelOf(castEntityClass);
    }

    @Override
    protected <T> Object resolveCommandTarget(CommandMessage msg,
                                              T parent,
                                              Member member,
                                              EntityModel<Object> childEntityModel) {
        return ReflectionUtils.getMemberValue(member, parent);
    }

    @Override
    protected <T> Stream<Object> resolveEventTargets(EventMessage message,
                                                     T parentEntity,
                                                     Member member,
                                                     ForwardingMode eventForwardingMode) {
        Object memberValue = ReflectionUtils.getMemberValue(member, parentEntity);
        return memberValue == null
                ? Stream.empty()
                : eventForwardingMode.filterCandidates(message, Stream.of(memberValue));
    }
}
