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

package io.axoniq.framework.springboot.autoconfig;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import io.axoniq.framework.messaging.eventhandling.deadletter.jdbc.DeadLetterSchema;
import io.axoniq.framework.messaging.eventhandling.deadletter.jdbc.JdbcSequencedDeadLetterQueue;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Spring Boot autoconfiguration that registers a JDBC-backed {@link SequencedDeadLetterQueueFactory} bean.
 * <p>
 * This configuration activates when a {@code DataSource} bean is present (i.e. JDBC is on the
 * classpath and configured). The registered factory creates a {@link JdbcSequencedDeadLetterQueue}
 * instance per event handling component, using the application's {@code DataSource},
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
 * To replace the default JDBC factory with a custom backend, declare your own {@link SequencedDeadLetterQueueFactory}
 * bean — the {@code @ConditionalOnMissingBean} guard on the default will yield to it.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see SequencedDeadLetterQueueFactory
 * @see JdbcSequencedDeadLetterQueue
 */
@AutoConfiguration(
        afterName = {
                "org.axonframework.extension.springboot.autoconfig.JdbcAutoConfiguration",
                "org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration",
                "io.axoniq.framework.springboot.autoconfig.JpaDeadLetterQueueAutoConfiguration"
        }
)
@ConditionalOnClass({DataSource.class, SequencedDeadLetterQueueFactory.class})
@ConditionalOnBean(DataSource.class)
public class JdbcDeadLetterQueueAutoConfiguration {

    /**
     * Creates a default {@link DeadLetterSchema} bean using the default column and table names.
     * <p>
     * To customize the schema, declare your own {@link DeadLetterSchema} bean — the
     * {@code @ConditionalOnMissingBean} guard on this default will yield to it.
     *
     * @return A {@link DeadLetterSchema} with default table and column names.
     */
    @Bean
    @ConditionalOnMissingBean
    public DeadLetterSchema deadLetterSchema() {
        return DeadLetterSchema.defaultSchema();
    }

    /**
     * Creates a JDBC-backed {@link SequencedDeadLetterQueueFactory} that instantiates a
     * {@link JdbcSequencedDeadLetterQueue} per event handling component.
     * <p>
     * The {@code processingGroup} passed to the factory is a component-scoped identifier following the pattern
     * {@code "DeadLetterQueue[processorName][componentName]"}, used to scope dead letters in the database.
     * The {@code configuration} parameter is ignored in this Spring implementation since all dependencies are
     * wired via Spring bean injection.
     *
     * @param dataSource       The JDBC {@link DataSource} used for persistence.
     * @param eventConverter   The {@link EventConverter} used to convert event payloads and metadata.
     * @param genericConverter The generic {@link Converter} used for type conversion.
     * @param schema           The {@link DeadLetterSchema} describing the table and column names.
     * @return A {@link SequencedDeadLetterQueueFactory} backed by JDBC.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({EventConverter.class, Converter.class})
    public SequencedDeadLetterQueueFactory jdbcDeadLetterQueueFactory(DataSource dataSource,
                                                                      EventConverter eventConverter,
                                                                      Converter genericConverter,
                                                                      DeadLetterSchema schema) {
        var provider = new JdbcTransactionalExecutorProvider(dataSource);
        return (processingGroup, configuration) -> JdbcSequencedDeadLetterQueue.builder()
                                                                                .processingGroup(processingGroup)
                                                                                .transactionalExecutorProvider(provider)
                                                                                .eventConverter(eventConverter)
                                                                                .genericConverter(genericConverter)
                                                                                .schema(schema)
                                                                                .build();
    }
}
