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

package io.axoniq.framework.springboot.autoconfig;

import jakarta.persistence.EntityManagerFactory;
import org.axonframework.conversion.Converter;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import org.axonframework.extension.springboot.util.RegisterDefaultEntities;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import io.axoniq.framework.messaging.eventhandling.deadletter.jpa.JpaSequencedDeadLetterQueue;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot autoconfiguration that registers a JPA-backed {@link SequencedDeadLetterQueueFactory} bean.
 * <p>
 * This configuration activates when a {@code EntityManagerFactory} bean is present (i.e. JPA is on the
 * classpath and configured). The registered factory creates a {@link JpaSequencedDeadLetterQueue}
 * instance per event handling component, using the application's {@code EntityManagerFactory},
 * {@link EventConverter}, and {@link Converter}. Each queue is scoped by a component-level processing
 * group identifier (e.g. {@code "DeadLetterQueue[myProcessor][0]"}).
 * <p>
 * To enable Dead Letter Queue processing for a specific processor, set:
 * <pre>{@code
 * axon.eventhandling.processors.<processorName>.dlq.enabled=true
 * }</pre>
 * <p>
 * The cache size used for sequence identifier caching can be tuned per processor:
 * <pre>{@code
 * axon.eventhandling.processors.<processorName>.dlq.cache.size=2048
 * }</pre>
 * <p>
 * To replace the default JPA factory with a custom backend, declare your own {@link SequencedDeadLetterQueueFactory}
 * bean — the {@code @ConditionalOnMissingBean} guard on the default will yield to it.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see SequencedDeadLetterQueueFactory
 * @see JpaSequencedDeadLetterQueue
 */
@AutoConfiguration(
        afterName = {
                "org.axonframework.extension.springboot.autoconfig.JpaAutoConfiguration",
                "org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration"
        }
)
@ConditionalOnClass({EntityManagerFactory.class, SequencedDeadLetterQueueFactory.class})
@ConditionalOnBean(EntityManagerFactory.class)
@RegisterDefaultEntities(packages = {
        "io.axoniq.framework.messaging.eventhandling.deadletter.jpa",
})
public class JpaDeadLetterQueueAutoConfiguration {

    /**
     * Creates a JPA-backed {@link SequencedDeadLetterQueueFactory} that instantiates a
     * {@link JpaSequencedDeadLetterQueue} per event handling component.
     * <p>
     * The {@code processingGroup} passed to the factory is a component-scoped identifier following the pattern
     * {@code "DeadLetterQueue[processorName][componentName]"}, used to scope dead letters in the database.
     * The {@code configuration} parameter is ignored in this Spring implementation since all dependencies are
     * wired via Spring bean injection.
     *
     * @param entityManagerFactory The JPA {@link EntityManagerFactory} used for persistence.
     * @param eventConverter       The {@link EventConverter} used to convert event payloads and metadata.
     * @param genericConverter     The generic {@link Converter} used for type conversion.
     * @return A {@link SequencedDeadLetterQueueFactory} backed by JPA.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({EventConverter.class, Converter.class})
    public SequencedDeadLetterQueueFactory jpaDeadLetterQueueFactory(EntityManagerFactory entityManagerFactory,
                                                                     EventConverter eventConverter,
                                                                     Converter genericConverter) {
        var provider = new JpaTransactionalExecutorProvider(entityManagerFactory);
        return (processingGroup, configuration) -> JpaSequencedDeadLetterQueue.builder()
                                                                               .processingGroup(processingGroup)
                                                                               .transactionalExecutorProvider(provider)
                                                                               .eventConverter(eventConverter)
                                                                               .genericConverter(genericConverter)
                                                                               .build();
    }
}
