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

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.modelling.annotation.AnnotationBasedEntityIdResolver;
import org.axonframework.modelling.annotation.EntityIdResolverDefinition;
import org.axonframework.modelling.EntityIdResolver;
import org.axonframework.modelling.annotation.TargetEntityId;

/**
 * {@link EntityIdResolverDefinition} that converts the payload of incoming messages based on the
 * {@link AnnotatedEntityMetamodel#getExpectedRepresentation(QualifiedName) expected payload type} of the message
 * handler in the model, and then looks for a {@link TargetEntityId}-annotated
 * member in the payload, through the {@link AnnotationBasedEntityIdResolver}.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public class AnnotatedEntityIdResolverDefinition implements EntityIdResolverDefinition {

    @Override
    public <E, ID> EntityIdResolver<ID> createIdResolver(Class<E> entityType,
                                                         Class<ID> idType,
                                                         AnnotatedEntityMetamodel<E> entityMetamodel,
                                                         Configuration configuration) {
        return new AnnotatedEntityIdResolver<>(
                entityMetamodel,
                idType,
                configuration.getComponent(MessageConverter.class),
                new AnnotationBasedEntityIdResolver<>()
        );
    }
}
