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
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.SimpleQueryBus;
import org.axonframework.messaging.queryhandling.SubscriptionQueryAlreadyRegisteredException;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An {@link AbstractSubscriptionQueryTestSuite} implementation validating the {@link SimpleQueryBus}.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 */
public class SimpleQueryBusSubscriptionQueryTest extends AbstractSubscriptionQueryTestSuite {

    private final Configuration config = createMessagingConfigurer().build();

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

    //fixme: SimpleQueryBus throws for duplicated subscriptions, how it should work with AxonServer?
    @Test
    void doubleSubscriptionMessage() {
        // given
        QueryMessage queryMessage = new GenericQueryMessage(
                CHAT_MESSAGES_QUERY_TYPE, TEST_QUERY_PAYLOAD
        );

        // when
        queryBus.subscriptionQuery(queryMessage, null, 50);
        MessageStream<QueryResponseMessage> secondSubscription = queryBus.subscriptionQuery(queryMessage, null, 50);

        // then
        assertTrue(secondSubscription.error().isPresent());
        assertInstanceOf(SubscriptionQueryAlreadyRegisteredException.class, secondSubscription.error().get());
    }
}
