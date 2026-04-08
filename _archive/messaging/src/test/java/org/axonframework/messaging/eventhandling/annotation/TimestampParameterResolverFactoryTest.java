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

package org.axonframework.messaging.eventhandling.annotation;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Instant;
import java.time.temporal.Temporal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Allard Buijze
 */
class TimestampParameterResolverFactoryTest {

    private TimestampParameterResolverFactory testSubject;

    private Method instantMethod;
    private Method temporalMethod;
    private Method stringMethod;
    private Method nonAnnotatedInstantMethod;
    private Method metaAnnotatedMethod;

    @BeforeEach
    void setUp() throws Exception {
        testSubject = new TimestampParameterResolverFactory();
        instantMethod = getClass().getMethod("someInstantMethod", Instant.class);
        metaAnnotatedMethod = getClass().getMethod("someMetaAnnotatedInstantMethod", Instant.class);
        nonAnnotatedInstantMethod = getClass().getMethod("someNonAnnotatedInstantMethod", Instant.class);
        temporalMethod = getClass().getMethod("someTemporalMethod", Temporal.class);
        stringMethod = getClass().getMethod("someStringMethod", String.class);
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public void someInstantMethod(@Timestamp Instant timestamp) {
        //Used in setUp()
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public void someMetaAnnotatedInstantMethod(@CustomTimestamp Instant timestamp) {
        //Used in setUp()
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public void someNonAnnotatedInstantMethod(Instant timestamp) {
        //Used in setUp()
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public void someTemporalMethod(@Timestamp Temporal timestamp) {
        //Used in setUp()
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public void someStringMethod(@Timestamp String timestamp) {
        //Used in setUp()
    }

    @Test
    void resolvesToDateTimeWhenAnnotated() {
        ParameterResolver<Instant> resolver =
                testSubject.createInstance(instantMethod, instantMethod.getParameters(), 0);

        final EventMessage message = EventTestUtils.asEventMessage("test");
        ProcessingContext context = StubProcessingContext.forMessage(message);
        assertTrue(resolver.matches(context));
        assertEquals(message.timestamp(), resolver.resolveParameterValue(context).join());
    }

    @Test
    void resolvesToReadableInstantWhenAnnotated() {
        ParameterResolver<Instant> resolver =
                testSubject.createInstance(temporalMethod, temporalMethod.getParameters(), 0);

        final EventMessage message = EventTestUtils.asEventMessage("test");
        ProcessingContext context = StubProcessingContext.forMessage(message);
        assertTrue(resolver.matches(context));
        assertEquals(message.timestamp(), resolver.resolveParameterValue(context).join());
    }

    @Test
    void ignoredWhenNotAnnotated() {
        ParameterResolver resolver =
                testSubject.createInstance(nonAnnotatedInstantMethod, nonAnnotatedInstantMethod.getParameters(), 0);
        assertNull(resolver);
    }

    @Test
    void ignoredWhenWrongType() {
        ParameterResolver resolver = testSubject.createInstance(stringMethod, stringMethod.getParameters(), 0);
        assertNull(resolver);
    }

    @Test
    void resolvesToDateTimeWhenAnnotatedWithMetaAnnotation() {
        Parameter[] parameters = metaAnnotatedMethod.getParameters();
        ParameterResolver<?> resolver = testSubject.createInstance(metaAnnotatedMethod, parameters, 0);
        final EventMessage message = EventTestUtils.asEventMessage("test");
        ProcessingContext context = StubProcessingContext.forMessage(message);
        assertTrue(resolver.matches(context), "Resolver should be a match for message " + message);
        assertEquals(message.timestamp(), resolver.resolveParameterValue(context).join());
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
    @Timestamp
    private @interface CustomTimestamp {

    }
}
