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

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.common.annotation.Internal;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.type.AnnotationMetadata;

/**
 * Adds the package holding the JPA saga entities ({@code SagaEntry} and {@code AssociationValueEntry}, in
 * {@code org.axonframework.modelling.saga.repository.jpa}) to Spring Boot's auto-configuration packages, so the
 * persistence unit maps them even though they live in the {@code axon-legacy} jar rather than the application's own
 * base packages.
 * <p>
 * Uses {@link AutoConfigurationPackages#register(BeanDefinitionRegistry, String...)}, which appends to the existing
 * set of auto-configuration packages. Contrast with
 * {@link org.springframework.boot.autoconfigure.domain.EntityScan @EntityScan}, which registers
 * {@code EntityScanPackages} and thereby overrides -- rather than augments -- the default scan, silently hiding the
 * application's own entities. This registrar therefore hardcodes the single legacy saga package rather than relying
 * on {@code @EntityScan}.
 * <p>
 * This class is internal: it is imported by {@link LegacyJpaSagaStoreAutoConfiguration} and is not meant to be used
 * directly by applications.
 *
 * @author Mateusz Nowak
 * @since 5.4.0
 */
@Internal
@Order(Ordered.HIGHEST_PRECEDENCE)
class LegacySagaJpaEntityRegistrar implements ImportBeanDefinitionRegistrar {

    private static final String SAGA_JPA_ENTITY_PACKAGE = "org.axonframework.modelling.saga.repository.jpa";

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        AutoConfigurationPackages.register(registry, SAGA_JPA_ENTITY_PACKAGE);
    }
}
