/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.deadletter;

import io.axoniq.framework.messaging.eventhandling.deadletter.jdbc.JdbcSequencedDeadLetterQueue;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviderUtil;
import io.axoniq.framework.messaging.multitenancy.deadletter.TenantAwareSequencedDeadLetterQueueFactory;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

public class SpringTenantDeadLetterQueueConfiguration {

    // tag::factory[]
    @Bean
    public TenantAwareSequencedDeadLetterQueueFactory tenantAwareDeadLetterQueueFactory() {
        return (tenant, processorName, queueConfiguration) -> createQueue(tenant, processorName, queueConfiguration);
    }

    private JdbcSequencedDeadLetterQueue<EventMessage> createQueue(
            TenantDescriptor tenant,
            String processorName,
            Configuration configuration
    ) {
        TenantComponentProvider<DataSource> dataSourceProvider =
                TenantComponentProviderUtil.find(configuration, DataSource.class)
                                           .orElseThrow();
        JdbcTransactionalExecutorProvider executor =
                new JdbcTransactionalExecutorProvider(dataSourceProvider.componentFor(tenant));

        return JdbcSequencedDeadLetterQueue.<EventMessage>builder()
                                           .processingGroup(processorName)
                                           .transactionalExecutorProvider(ignored -> executor.getTransactionalExecutor(
                                                   null
                                           ))
                                           .eventConverter(configuration.getComponent(EventConverter.class))
                                           .genericConverter(configuration.getComponent(GeneralConverter.class))
                                           .build();
    }
    // end::factory[]
}
