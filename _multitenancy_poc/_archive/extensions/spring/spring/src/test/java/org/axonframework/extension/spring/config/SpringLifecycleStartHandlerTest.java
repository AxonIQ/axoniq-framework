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

package org.axonframework.extension.spring.config;

import org.axonframework.extension.spring.config.SpringLifecycleStartHandler;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link SpringLifecycleStartHandler}.
 *
 * @author Steven van Beelen
 */
class SpringLifecycleStartHandlerTest {

    private Supplier<CompletableFuture<?>> action;

    private SpringLifecycleStartHandler testSubject;

    @BeforeEach
    void setUp() {
        //noinspection unchecked
        action = mock();
        doReturn(CompletableFuture.completedFuture(null)).when(action)
                                                         .get();

        testSubject = new SpringLifecycleStartHandler(42, action);
    }

    @Test
    void phaseIsRegisteredCorrectly() {
        assertEquals(42, testSubject.getPhase());
    }

    @Test
    void isRunningReflectsCorrectState() {
        assertFalse(testSubject.isRunning());

        testSubject.start();
        assertTrue(testSubject.isRunning());

        testSubject.stop();
        assertFalse(testSubject.isRunning());
    }

    @Test
    void actionIsInvokedOnStart() {
        testSubject.start();

        verify(action).get();
    }
}