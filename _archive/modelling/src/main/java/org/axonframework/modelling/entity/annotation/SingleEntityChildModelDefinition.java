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

import org.axonframework.modelling.entity.EntityMetamodel;
import org.axonframework.modelling.entity.child.ChildEntityFieldDefinition;
import org.axonframework.modelling.entity.child.CommandTargetResolver;
import org.axonframework.modelling.entity.child.EntityChildMetamodel;
import org.axonframework.modelling.entity.child.EventTargetMatcher;
import org.axonframework.modelling.entity.child.SingleEntityChildMetamodel;

import java.lang.reflect.Member;

import static org.axonframework.common.ReflectionUtils.getMemberValueType;

/**
 * {@link EntityChildModelDefinition} that creates {@link EntityChildMetamodel} instances for child entities that are
 * represented as a single entity (not iterable). It resolves the child type from the member's type and creates a
 * {@link SingleEntityChildMetamodel} accordingly.
 * <p>
 * Before version 5.0.0, this class was known as the
 * {@code org.axonframework.modelling.command.inspection.AggregateMemberAnnotatedChildEntityDefinition}. The class has
 * been renamed to better fit the new entity modeling.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class SingleEntityChildModelDefinition extends AbstractEntityChildModelDefinition {

    @Override
    protected boolean isMemberTypeSupported(Class<?> memberType) {
        return !Iterable.class.isAssignableFrom(memberType);
    }

    @Override
    protected Class<?> getChildTypeFromMember(Member member) {
        return getMemberValueType(member);
    }

    @Override
    protected <C, P> EntityChildMetamodel<C, P> doCreate(
            Class<P> parentClass,
            EntityMetamodel<C> entityMetamodel,
            String fieldName,
            EventTargetMatcher<C> eventTargetMatcher,
            CommandTargetResolver<C> commandTargetResolver) {
        return SingleEntityChildMetamodel
                .forEntityModel(parentClass, entityMetamodel)
                .childEntityFieldDefinition(ChildEntityFieldDefinition.forFieldName(
                        parentClass, fieldName
                ))
                .commandTargetResolver(commandTargetResolver)
                .eventTargetMatcher(eventTargetMatcher)
                .build();
    }
}
