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

import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.extension.reactor.messaging.core.interception.ReactorDispatchInterceptorRegistry;
import org.axonframework.extension.reactor.messaging.core.ReactorMessageDispatchInterceptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link ReactorAutoConfiguration}.
 */
class ReactorAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ReactorAutoConfiguration.class));

    @Nested
    class DecoratorRegistration {

        @Test
        void registersDecoratorDefinitionWhenCommandInterceptorBeanPresent() {
            contextRunner
                    .withUserConfiguration(CommandInterceptorConfiguration.class)
                    .run(context -> assertThat(context).hasBean("reactorDispatchInterceptorEnhancer"));
        }

        @Test
        void registersDecoratorDefinitionWhenEventInterceptorBeanPresent() {
            contextRunner
                    .withUserConfiguration(EventInterceptorConfiguration.class)
                    .run(context -> assertThat(context).hasBean("reactorDispatchInterceptorEnhancer"));
        }

        @Test
        void registersDecoratorDefinitionWhenQueryInterceptorBeanPresent() {
            contextRunner
                    .withUserConfiguration(QueryInterceptorConfiguration.class)
                    .run(context -> assertThat(context).hasBean("reactorDispatchInterceptorEnhancer"));
        }

        @Test
        void registersDecoratorDefinitionWhenGenericMessageInterceptorBeanPresent() {
            contextRunner
                    .withUserConfiguration(GenericInterceptorConfiguration.class)
                    .run(context -> assertThat(context).hasBean("reactorDispatchInterceptorEnhancer"));
        }

        @Test
        void doesNotRegisterDecoratorDefinitionWhenNoInterceptorBeansPresent() {
            contextRunner
                    .run(context -> assertThat(context).doesNotHaveBean("reactorDispatchInterceptorEnhancer"));
        }
    }

    // -- Test configurations --

    @Configuration
    static class GenericInterceptorConfiguration {

        @Bean
        ReactorMessageDispatchInterceptor<Message> testGenericInterceptor() {
            return (message, context, chain) -> {
                var enriched = message.andMetadata(Metadata.with("intercepted", "true"));
                return chain.proceed(enriched, context);
            };
        }
    }

    @Configuration
    static class CommandInterceptorConfiguration {

        @Bean
        ReactorMessageDispatchInterceptor<CommandMessage> testCommandInterceptor() {
            return (message, context, chain) -> {
                var enriched = message.andMetadata(Metadata.with("intercepted", "true"));
                return chain.proceed(enriched, context);
            };
        }
    }

    @Configuration
    static class EventInterceptorConfiguration {

        @Bean
        ReactorMessageDispatchInterceptor<EventMessage> testEventInterceptor() {
            return (message, context, chain) -> {
                var enriched = message.andMetadata(Metadata.with("intercepted", "true"));
                return chain.proceed(enriched, context);
            };
        }
    }

    @Configuration
    static class QueryInterceptorConfiguration {

        @Bean
        ReactorMessageDispatchInterceptor<QueryMessage> testQueryInterceptor() {
            return (message, context, chain) -> {
                var enriched = message.andMetadata(Metadata.with("intercepted", "true"));
                return chain.proceed(enriched, context);
            };
        }
    }
}
