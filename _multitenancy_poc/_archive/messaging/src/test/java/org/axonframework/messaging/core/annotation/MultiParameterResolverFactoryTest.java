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

package org.axonframework.messaging.core.annotation;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;


import org.jspecify.annotations.Nullable;
import org.axonframework.common.Priority;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.mockito.*;

/**
 * @author Allard Buijze
 * @author Nakul Mishra
 */
class MultiParameterResolverFactoryTest {

    private ParameterResolverFactory mockFactory1;
    private ParameterResolverFactory mockFactory2;

    private ParameterResolver mockResolver1;
    private ParameterResolver mockResolver2;

    private MultiParameterResolverFactory testSubject;

    @BeforeEach
    void setUp() {
        mockFactory1 = mock(ParameterResolverFactory.class);
        mockFactory2 = mock(ParameterResolverFactory.class);

        mockResolver1 = mock(ParameterResolver.class);
        mockResolver2 = mock(ParameterResolver.class);

        when(mockFactory1.createInstance(ArgumentMatchers.any(Executable.class),
                                         ArgumentMatchers.any(),
                                         ArgumentMatchers.anyInt())).thenReturn(mockResolver1);
        when(mockFactory2.createInstance(ArgumentMatchers.any(Executable.class),
                                         ArgumentMatchers.any(),
                                         ArgumentMatchers.anyInt())).thenReturn(mockResolver2);

        testSubject = new MultiParameterResolverFactory(mockFactory1, mockFactory2);
    }

    @Test
    void resolversQueriedInOrderProvided() throws Exception {
        Method equals = getClass().getMethod("equals", Object.class);
        ParameterResolver factory = testSubject.createInstance(equals, equals.getParameters(), 0);
        assertNotNull(factory);
        ProcessingContext context = new StubProcessingContext();
        assertFalse(factory.matches(context));

        InOrder inOrder = inOrder(mockFactory1, mockFactory2, mockResolver1, mockResolver2);
        inOrder.verify(mockFactory1).createInstance(ArgumentMatchers.any(Executable.class),
                                                    ArgumentMatchers.any(),
                                                    ArgumentMatchers.anyInt());
        inOrder.verify(mockResolver1).matches(context);

        verify(mockFactory2, never()).createInstance(ArgumentMatchers.any(Executable.class),
                                                     ArgumentMatchers.any(),
                                                     ArgumentMatchers.anyInt());

        verify(mockResolver2, never()).matches(context);
    }

    @Test
    void firstMatchingResolverMayReturnValue() throws Exception {
        Method equals = getClass().getMethod("equals", Object.class);
        final EventMessage message = EventTestUtils.asEventMessage("test");
        ProcessingContext context = StubProcessingContext.forMessage(message);
        when(mockFactory1.createInstance(ArgumentMatchers.any(Executable.class),
                                         ArgumentMatchers.any(),
                                         ArgumentMatchers.anyInt()))
                .thenReturn(null);
        when(mockResolver2.matches(context)).thenReturn(true);
        when(mockResolver2.resolveParameterValue(context)).thenReturn(CompletableFuture.completedFuture("Resolved"));

        ParameterResolver factory = testSubject.createInstance(equals, equals.getParameters(), 0);
        assertTrue(factory.matches(context));
        assertEquals("Resolved", factory.resolveParameterValue(context).join());

        verify(mockResolver1, never()).resolveParameterValue(context);
    }

    @Test
    void nestedParameterResolversAreOrdered() {
        final LowPrioParameterResolverFactory lowPrio = new LowPrioParameterResolverFactory();
        final HighPrioParameterResolverFactory highPrio = new HighPrioParameterResolverFactory();
        testSubject = MultiParameterResolverFactory.ordered(mockFactory1,
                                                            new MultiParameterResolverFactory(lowPrio, mockFactory2),
                                                            new MultiParameterResolverFactory(highPrio));

        assertEquals(Arrays.asList(highPrio, mockFactory1, mockFactory2, lowPrio), testSubject.getDelegates());
    }

    @Priority(Priority.LOW)
    private static class LowPrioParameterResolverFactory extends AbstractNoopParameterResolverFactory {

    }

    @Priority(Priority.HIGH)
    private static class HighPrioParameterResolverFactory extends AbstractNoopParameterResolverFactory {

    }

    private static class AbstractNoopParameterResolverFactory implements ParameterResolverFactory {

        @Nullable
        @Override
        public ParameterResolver createInstance(@NonNull Executable executable, @NonNull Parameter[] parameters, int parameterIndex) {
            return null;
        }
    }


}
