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

import org.axonframework.messaging.core.ScopeDescriptor;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link ScopeDescriptorParameterResolverFactory}.
 *
 * @author Steven van Beelen
 */
class ScopeDescriptorParameterResolverFactoryTest {

    private final ScopeDescriptorParameterResolverFactory testSubject = new ScopeDescriptorParameterResolverFactory();

    private Method scopeDescriptorLessMethod;
    private Method scopeDescriptorUsingMethod;

    @BeforeEach
    void setUp() throws NoSuchMethodException {
        scopeDescriptorUsingMethod = getClass().getMethod("someScopeDescriptorUsingMethod", ScopeDescriptor.class);
        scopeDescriptorLessMethod = getClass().getMethod("someScopeDescriptorLessMethod", String.class);
    }

    @Test
    void parameterResolverIsNullForScopeDescriptorLessMethod() {
        assertNull(testSubject.createInstance(scopeDescriptorLessMethod, scopeDescriptorLessMethod.getParameters(), 0));
    }

    @Test
    void resolvesNoScopeDescriptor() {
//        ParameterResolver<ScopeDescriptor> resolver =
//                testSubject.createInstance(scopeDescriptorUsingMethod, scopeDescriptorUsingMethod.getParameters(), 0);

//        assertTrue(resolver.matches(new StubProcessingContext()));
//        assertEquals(NoScopeDescriptor.INSTANCE, resolver.resolveParameterValue(new StubProcessingContext()));
    }

    @SuppressWarnings("unused")
    public void someScopeDescriptorLessMethod(String s) {
        // Used for testing
    }

    @SuppressWarnings("unused")
    public void someScopeDescriptorUsingMethod(ScopeDescriptor scopeDescriptor) {
        // Used for testing
    }
}