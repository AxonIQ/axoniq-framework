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

package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DefaultTenantResolverRegistryTest {

    private static final TenantResolver<Message> GENERAL_RESOLVER = resolverReturning("general");
    private static final TenantResolver<Message> REPLACEMENT_GENERAL_RESOLVER =
            resolverReturning("replacement-general");
    private static final TenantResolver<CommandMessage> COMMAND_RESOLVER = resolverReturning("command");
    private static final TenantResolver<CommandMessage> REPLACEMENT_COMMAND_RESOLVER =
            resolverReturning("replacement-command");
    private static final TenantResolver<EventMessage> EVENT_RESOLVER = resolverReturning("event");
    private static final TenantResolver<QueryMessage> QUERY_RESOLVER = resolverReturning("query");

    private final Configuration configuration = mock(Configuration.class);
    private final TenantResolverRegistry testSubject = TenantResolverRegistry.create();

    @Nested
    class WithoutRegisteredResolvers {

        @Test
        void hasNoResolver() {
            // when
            boolean result = testSubject.hasResolver();

            // then
            assertThat(result).isFalse();
        }

        @Test
        void resolvesNothing() {
            // when
            TenantResolver<Message> generalResolver = testSubject.resolver(configuration);
            TenantResolver<Message> commandResolver = testSubject.commandResolver(configuration);
            TenantResolver<Message> eventResolver = testSubject.eventResolver(configuration);
            TenantResolver<Message> queryResolver = testSubject.queryResolver(configuration);

            // then
            assertThat(generalResolver).isNull();
            assertThat(commandResolver).isNull();
            assertThat(eventResolver).isNull();
            assertThat(queryResolver).isNull();
        }
    }

    @Nested
    class WithGeneralResolver {

        @Test
        void usesGeneralResolverForEveryMessageType() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER);

            // when
            TenantResolver<Message> generalResolver = testSubject.resolver(configuration);
            TenantResolver<Message> commandResolver = testSubject.commandResolver(configuration);
            TenantResolver<Message> eventResolver = testSubject.eventResolver(configuration);
            TenantResolver<Message> queryResolver = testSubject.queryResolver(configuration);

            // then
            assertThat(generalResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(commandResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(eventResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(queryResolver).isSameAs(GENERAL_RESOLVER);
        }

        @Test
        void reportsResolverPresent() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER);

            // when
            boolean result = testSubject.hasResolver();

            // then
            assertThat(result).isTrue();
        }

        @Test
        void replacesPreviouslyRegisteredGeneralResolver() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER);

            // when
            testSubject.registerResolver(config -> REPLACEMENT_GENERAL_RESOLVER);

            // then
            assertThat(testSubject.resolver(configuration)).isSameAs(REPLACEMENT_GENERAL_RESOLVER);
            assertThat(testSubject.commandResolver(configuration)).isSameAs(REPLACEMENT_GENERAL_RESOLVER);
            assertThat(testSubject.eventResolver(configuration)).isSameAs(REPLACEMENT_GENERAL_RESOLVER);
            assertThat(testSubject.queryResolver(configuration)).isSameAs(REPLACEMENT_GENERAL_RESOLVER);
        }
    }

    @Nested
    class WithTypeSpecificResolvers {

        @Test
        void commandResolverTakesPrecedenceOverGeneralResolver() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER)
                       .registerCommandResolver(config -> COMMAND_RESOLVER);

            // when
            TenantResolver<Message> commandResolver = testSubject.commandResolver(configuration);
            TenantResolver<Message> eventResolver = testSubject.eventResolver(configuration);
            TenantResolver<Message> queryResolver = testSubject.queryResolver(configuration);
            TenantResolver<Message> generalResolver = testSubject.resolver(configuration);

            // then
            assertThat(commandResolver).isSameAs(COMMAND_RESOLVER);
            assertThat(eventResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(queryResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(generalResolver).isSameAs(GENERAL_RESOLVER);
        }

        @Test
        void eventResolverTakesPrecedenceOverGeneralResolver() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER)
                       .registerEventResolver(config -> EVENT_RESOLVER);

            // when
            TenantResolver<Message> commandResolver = testSubject.commandResolver(configuration);
            TenantResolver<Message> eventResolver = testSubject.eventResolver(configuration);
            TenantResolver<Message> queryResolver = testSubject.queryResolver(configuration);
            TenantResolver<Message> generalResolver = testSubject.resolver(configuration);

            // then
            assertThat(commandResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(eventResolver).isSameAs(EVENT_RESOLVER);
            assertThat(queryResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(generalResolver).isSameAs(GENERAL_RESOLVER);
        }

        @Test
        void queryResolverTakesPrecedenceOverGeneralResolver() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER)
                       .registerQueryResolver(config -> QUERY_RESOLVER);

            // when
            TenantResolver<Message> commandResolver = testSubject.commandResolver(configuration);
            TenantResolver<Message> eventResolver = testSubject.eventResolver(configuration);
            TenantResolver<Message> queryResolver = testSubject.queryResolver(configuration);
            TenantResolver<Message> generalResolver = testSubject.resolver(configuration);

            // then
            assertThat(commandResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(eventResolver).isSameAs(GENERAL_RESOLVER);
            assertThat(queryResolver).isSameAs(QUERY_RESOLVER);
            assertThat(generalResolver).isSameAs(GENERAL_RESOLVER);
        }

        @Test
        void allTypeSpecificResolversCanBeRegisteredTogether() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER)
                       .registerCommandResolver(config -> COMMAND_RESOLVER)
                       .registerEventResolver(config -> EVENT_RESOLVER)
                       .registerQueryResolver(config -> QUERY_RESOLVER);

            // when
            TenantResolver<Message> commandResolver = testSubject.commandResolver(configuration);
            TenantResolver<Message> eventResolver = testSubject.eventResolver(configuration);
            TenantResolver<Message> queryResolver = testSubject.queryResolver(configuration);
            TenantResolver<Message> generalResolver = testSubject.resolver(configuration);

            // then
            assertThat(commandResolver).isSameAs(COMMAND_RESOLVER);
            assertThat(eventResolver).isSameAs(EVENT_RESOLVER);
            assertThat(queryResolver).isSameAs(QUERY_RESOLVER);
            assertThat(generalResolver).isSameAs(GENERAL_RESOLVER);
        }

        @Test
        void typeSpecificResolverWithoutGeneralResolverOnlyAppliesToItsMessageType() {
            // given
            testSubject.registerCommandResolver(config -> COMMAND_RESOLVER);

            // when
            boolean hasResolver = testSubject.hasResolver();
            TenantResolver<Message> commandResolver = testSubject.commandResolver(configuration);
            TenantResolver<Message> eventResolver = testSubject.eventResolver(configuration);
            TenantResolver<Message> queryResolver = testSubject.queryResolver(configuration);
            TenantResolver<Message> generalResolver = testSubject.resolver(configuration);

            // then
            assertThat(hasResolver).isTrue();
            assertThat(commandResolver).isSameAs(COMMAND_RESOLVER);
            assertThat(eventResolver).isNull();
            assertThat(queryResolver).isNull();
            assertThat(generalResolver).isNull();
        }

        @Test
        void replacesPreviouslyRegisteredTypeSpecificResolver() {
            // given
            testSubject.registerResolver(config -> GENERAL_RESOLVER)
                       .registerCommandResolver(config -> COMMAND_RESOLVER);

            // when
            testSubject.registerCommandResolver(config -> REPLACEMENT_COMMAND_RESOLVER);

            // then
            assertThat(testSubject.commandResolver(configuration)).isSameAs(REPLACEMENT_COMMAND_RESOLVER);
            assertThat(testSubject.eventResolver(configuration)).isSameAs(GENERAL_RESOLVER);
            assertThat(testSubject.queryResolver(configuration)).isSameAs(GENERAL_RESOLVER);
        }
    }

    @Nested
    class BuildsResolversOnce {

        @Test
        void buildsTheGeneralResolverOnlyOnceAcrossAccessorCalls() {
            // given a builder producing a fresh resolver on every invocation
            AtomicInteger buildCount = new AtomicInteger();
            testSubject.registerResolver(config -> {
                buildCount.incrementAndGet();
                return resolverReturning("general-" + buildCount.get());
            });

            // when every accessor falls back to the general resolver
            TenantResolver<Message> firstAccess = testSubject.resolver(configuration);
            TenantResolver<Message> secondAccess = testSubject.resolver(configuration);
            TenantResolver<Message> commandFallback = testSubject.commandResolver(configuration);

            // then
            assertThat(buildCount).hasValue(1);
            assertThat(secondAccess).isSameAs(firstAccess);
            assertThat(commandFallback).isSameAs(firstAccess);
        }

        @Test
        void buildsATypeSpecificResolverOnlyOnceAcrossAccessorCalls() {
            // given a builder producing a fresh resolver on every invocation
            AtomicInteger buildCount = new AtomicInteger();
            testSubject.registerCommandResolver(config -> {
                buildCount.incrementAndGet();
                return resolverReturning("command-" + buildCount.get());
            });

            // when
            TenantResolver<Message> firstAccess = testSubject.commandResolver(configuration);
            TenantResolver<Message> secondAccess = testSubject.commandResolver(configuration);

            // then
            assertThat(buildCount).hasValue(1);
            assertThat(secondAccess).isSameAs(firstAccess);
        }
    }

    private static <M extends Message> TenantResolver<M> resolverReturning(String tenantId) {
        return (message, processingContext, tenants) -> TenantDescriptor.tenantWithId(tenantId);
    }
}
