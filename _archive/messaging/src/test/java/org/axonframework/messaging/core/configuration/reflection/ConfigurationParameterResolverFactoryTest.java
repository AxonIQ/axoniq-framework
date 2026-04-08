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

package org.axonframework.messaging.core.configuration.reflection;

import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Optional;

import static org.axonframework.messaging.commandhandling.CommandBusTestUtils.aCommandBus;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link ConfigurationParameterResolverFactory}.
 *
 * @author Allard Buijze
 */
class ConfigurationParameterResolverFactoryTest {

    private Method method;
    private Parameter[] parameters;
    private Configuration configuration;
    private CommandBus commandBus;

    private ConfigurationParameterResolverFactory testSubject;

    @BeforeEach
    void setUp() throws Exception {
        method = getClass().getMethod("donorMethod", String.class, CommandBus.class);
        parameters = method.getParameters();
        configuration = mock(Configuration.class);
        commandBus = aCommandBus();
        when(configuration.getOptionalComponent(CommandBus.class)).thenReturn(Optional.of(commandBus));

        testSubject = new ConfigurationParameterResolverFactory(configuration);
    }

    @Test
    void returnsNullOnUnavailableParameter() {
        assertNull(testSubject.createInstance(method, parameters, 0));

        verify(configuration).getOptionalComponent(String.class);
    }

    @Test
    void configurationContainsRequestedParameter() {
        ParameterResolver<?> actual = testSubject.createInstance(method, parameters, 1);
        Message testMessage = new GenericMessage(new MessageType("message"), "test");

        assertNotNull(actual);
        assertSame(commandBus, actual.resolveParameterValue(StubProcessingContext.forMessage(testMessage)).join());

        verify(configuration).getOptionalComponent(CommandBus.class);
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void donorMethod(String type, CommandBus commandBus) {

    }
}
