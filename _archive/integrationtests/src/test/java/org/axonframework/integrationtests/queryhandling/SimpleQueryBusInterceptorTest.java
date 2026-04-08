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

package org.axonframework.integrationtests.queryhandling;

import org.axonframework.axonserver.connector.AxonServerConfigurationEnhancer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.SimpleQueryBus;
import org.junit.jupiter.api.AfterEach;

/**
 * An {@link AbstractQueryInterceptorTestSuite} implementation validating query interceptor functionality with the
 * {@link SimpleQueryBus}.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
public class SimpleQueryBusInterceptorTest extends AbstractQueryInterceptorTestSuite {

    private final AxonConfiguration config = createMessagingConfigurer().build();

    @Override
    public QueryBus queryBus() {
        return config.getComponent(QueryBus.class);
    }

    @Override
    protected MessagingConfigurer createMessagingConfigurer() {
        return MessagingConfigurer.create()
                .componentRegistry(cr -> cr.disableEnhancer(
                        AxonServerConfigurationEnhancer.class));
    }

    @AfterEach
    void tearDown() {
        config.shutdown();
    }
}
