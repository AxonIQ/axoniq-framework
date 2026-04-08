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

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.messaging.core.unitofwork.transaction.NoTransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration class that registers a bean creation method for a default {@link TransactionManager}, the
 * {@link NoTransactionManager}.
 *
 * @author Allard Buijze
 * @since 3.0.3
 */
@AutoConfiguration
@AutoConfigureAfter({JpaTransactionAutoConfiguration.class, JdbcTransactionAutoConfiguration.class})
public class NoOpTransactionAutoConfiguration {

    /**
     * Bean creation method constructing the default {@link TransactionManager} to be used by Axon Framework.
     * <p>
     * The default is a {@link NoTransactionManager}.
     *
     * @return The {@link TransactionManager} to be used by Axon Framework.
     */
    @Bean
    @ConditionalOnMissingBean(TransactionManager.class)
    public TransactionManager axonTransactionManager() {
        return NoTransactionManager.INSTANCE;
    }
}
