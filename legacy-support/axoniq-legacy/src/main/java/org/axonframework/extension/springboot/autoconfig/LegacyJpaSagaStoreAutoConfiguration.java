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

import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.jpa.JpaSagaStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

/**
 * Autoconfiguration class that registers a {@link JpaSagaStore} when JPA infrastructure is available, ported from
 * Axon Framework 4's JPA saga store wiring onto {@code axon-legacy}'s {@link JpaSagaStore} builder.
 * <p>
 * Registers the {@code SagaEntry} and {@code AssociationValueEntry} entities defined in
 * {@code org.axonframework.modelling.saga.repository.jpa} with the persistence unit via
 * {@link LegacySagaJpaEntityRegistrar}, so they are picked up even though they live outside the application's own
 * base packages.
 * <p>
 * The {@link #sagaStore(GeneralConverter, EntityManagerProvider)} bean is {@link Lazy}: {@link JpaSagaStore}'s
 * constructor eagerly registers named queries against the {@link jakarta.persistence.EntityManager}, which requires
 * a fully initialized persistence unit. Axon Framework 4 deferred this bean the same way for the same reason.
 * <p>
 * Runs after {@code org.axonframework.extension.springboot.autoconfig.JpaAutoConfiguration} so the
 * {@link EntityManagerProvider} bean it provides is available, and before {@link LegacySagaAutoConfiguration} so this
 * bean is visible when that class's in-memory fallback checks for an existing {@link SagaStore}. The ordering
 * reference to {@code JpaAutoConfiguration} uses {@link AutoConfiguration#afterName()} rather than
 * {@link AutoConfiguration#after()} so that {@code axon-legacy} carries no compile-time dependency on the
 * {@code axon-spring-boot-autoconfigure} module.
 *
 * @author Mateusz Nowak
 * @since 5.4.0
 */
@AutoConfiguration(afterName = {
        "org.axonframework.extension.springboot.autoconfig.JpaAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
        "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"
})
@ConditionalOnClass({JpaSagaStore.class, EntityManagerFactory.class})
@ConditionalOnBean(EntityManagerFactory.class)
@Import(LegacySagaJpaEntityRegistrar.class)
public class LegacyJpaSagaStoreAutoConfiguration {

    /**
     * Provides a {@link JpaSagaStore} using the given {@code converter} and {@code entityManagerProvider}.
     * <p>
     * Named {@code sagaStore}, matching Axon Framework 4's bean name, so a manually declared {@link SagaStore} bean
     * of that same name (rather than type) still triggers {@link ConditionalOnMissingBean}'s type-based check
     * correctly since {@link SagaStore} is the condition target, not the bean name.
     *
     * @param converter              the converter used to convert a saga instance to and from its serialized form
     * @param entityManagerProvider  the provider of the {@link jakarta.persistence.EntityManager} used to access the
     *                               underlying database
     * @return a JPA-backed {@link SagaStore}
     */
    @Lazy
    @Bean
    @ConditionalOnMissingBean(SagaStore.class)
    public JpaSagaStore sagaStore(GeneralConverter converter, EntityManagerProvider entityManagerProvider) {
        return JpaSagaStore.builder()
                            .converter(converter)
                            .entityManagerProvider(entityManagerProvider)
                            .build();
    }
}
