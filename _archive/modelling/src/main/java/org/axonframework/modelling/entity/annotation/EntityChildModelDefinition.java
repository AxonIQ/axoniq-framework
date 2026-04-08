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
import org.axonframework.modelling.entity.child.EntityChildMetamodel;

import java.lang.reflect.Member;
import java.util.Optional;

/**
 * Interface describing the definition of an {@link EntityChildMetamodel}. These definitions are automatically
 * detected by the {@link AnnotatedEntityMetamodel} if the definition's implementation is registered in the
 * {@code META-INF/services/org.axonframework.modelling.entity.annotation.EntityChildModelDefinition} file.
 * <p>
 * Note: This class was known as {code org.axonframework.modelling.command.inspection.ChildEntityDefinition} before
 * version 5.0.0.
 *
 * @author Allard Buijze
 * @author Mitchell Herrijgers
 * @see java.util.ServiceLoader
 * @since 3.0.0
 */
public interface EntityChildModelDefinition {

    /**
     * Inspect the given {@code member}, which is declared on the given {@code parentClass} for the presence of a child
     * entity according to this definition. If a child entity is found, an {@link EntityChildMetamodel} is
     * returned. This metamodel can use the given {@code metamodelFactory} to create the child
     * {@link EntityMetamodel} based on the class.
     *
     * @param parentClass      The class of the parent entity.
     * @param metamodelFactory A factory to create the child {@link EntityMetamodel} based on the class.
     * @param member           The member to inspect for a child entity.
     * @param <C>              The type of the child entity.
     * @param <P>              The type of the parent entity.
     * @return An {@link Optional} that resolves to an {@link EntityChildMetamodel} if the field represents a
     * child entity, or an empty optional if no child entity is found.
     */
    <C, P> Optional<EntityChildMetamodel<C, P>> createChildDefinition(
            Class<P> parentClass,
            AnnotatedEntityMetamodelFactory metamodelFactory,
            Member member
    );
}
