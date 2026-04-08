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

package org.axonframework.extension.springboot;

import org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition;
import org.axonframework.extension.spring.authorization.SecuredMessageHandlerDefinition;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.jupiter.api.Assertions.*;

class SecurityAutoConfigurationTest {

    @Test
    void handlerEnhancerDefinitionIsRegistered() {
        new ApplicationContextRunner()
                .withUserConfiguration(Context.class)
                .withPropertyValues("axon.axonserver.enabled=false")
                .run(context -> {
                    assertNotNull(context);

                    assertTrue(context.containsBean("securedMessageHandlerDefinition"));
                    assertTrue(context.getBeansOfType(HandlerEnhancerDefinition.class).containsKey("securedMessageHandlerDefinition"));
                    assertFalse(context.getBeansOfType(SecuredMessageHandlerDefinition.class).isEmpty());
                });
    }

    @Test
    void handlerEnhancerDefinitionIsNotRegisteredWithoutSecuredAnnotation() {
        new ApplicationContextRunner()
                .withClassLoader(new FilteredClassLoader("org.springframework.security.access.annotation.Secured"))
                .withUserConfiguration(Context.class)
                .run(context -> assertFalse(context.containsBean("securedMessageHandlerDefinition")));
    }

    @EnableAutoConfiguration
    @ContextConfiguration
    public static class Context {

    }
}
