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

package org.axonframework.spring.stereotype;

import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that informs Axon's auto configurer for Spring that a given {@link Component} is a saga instance.
 * <p>
 * Requires the {@code axon-legacy} module on the classpath, which carries the Saga itself. The Saga is registered on
 * an event processor named after the Saga type, {@code <SagaName>Processor}, unless a
 * {@link org.axonframework.messaging.core.annotation.Namespace @Namespace} on the Saga or an explicit
 * {@code EventProcessorDefinition} says otherwise. That processor is configured through the regular
 * {@code axon.eventhandling.processors.<SagaName>Processor} properties. Sagas take part in the same processor
 * assignment pass as ordinary event handlers, so a Saga and an ordinary handler resolving to the same processor name
 * share that processor, as they did under one {@code @ProcessingGroup} in Axon Framework 4.
 * <p>
 * The stereotype only lets Spring discover the type; Axon owns the lifecycle of Saga instances. Spring
 * collaborators are resolved as parameters of a {@link SagaEventHandler @SagaEventHandler} method, not injected into
 * Saga fields. A Saga type must therefore be public and expose an accessible no-argument constructor.
 * <p>
 * Sagas carry the Axon Framework 4 API, to ease migration of projects that cannot move off it in one go. This
 * annotation keeps its Axon Framework 4 package for that reason, so migrating Saga classes component-scan unchanged.
 *
 * @author Allard Buijze
 * @since 3.0
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Component
@Scope("prototype")
public @interface Saga {

    /**
     * Selects the name of the {@link SagaStore} bean. If left empty the saga will be stored in the Saga Store
     * configured in the global Axon Configuration.
     *
     * @return the name of the {@link SagaStore} bean to keep Sagas of this type in
     */
    String sagaStore() default "";
}
