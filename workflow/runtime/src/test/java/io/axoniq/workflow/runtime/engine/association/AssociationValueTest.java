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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.association;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests the {@link AssociationValue}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class AssociationValueTest {

    private final ValueRetriever retriever = Mockito.mock(ValueRetriever.class);
    private final ValueComparisonOperator operator = new EqualsComparison();
    private final Converter converter = Mockito.mock(Converter.class);
    private final EventMessage eventMessage = Mockito.mock(EventMessage.class);

    @BeforeEach
    void setUp() {
        reset(retriever, converter, eventMessage);
    }

    @Test
    void shouldMatchWhenValueIsEqual() {
        String expectedValue = "123";
        AssociationValue associationValue = new AssociationValue(retriever, operator, () -> expectedValue);

        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn("123");

        Predicate<EventMessage> predicate = associationValue.asEventMessagePredicate(converter);

        assertTrue(predicate.test(eventMessage));
        verify(retriever).apply(eventMessage, converter);
    }

    @Test
    void shouldNotMatchWhenValueIsDifferent() {
        String expectedValue = "123";
        AssociationValue associationValue = new AssociationValue(retriever, operator, () -> expectedValue);

        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn("456");

        Predicate<EventMessage> predicate = associationValue.asEventMessagePredicate(converter);

        assertFalse(predicate.test(eventMessage));
    }

    @Test
    void shouldMatchWhenBothAreNull() {
        AssociationValue associationValue = new AssociationValue(retriever, operator, () -> null);

        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn(null);

        Predicate<EventMessage> predicate = associationValue.asEventMessagePredicate(converter);

        assertTrue(predicate.test(eventMessage));
    }

    @Test
    void shouldEvaluateSupplierOnEachCall() {
        final String[] valueHolder = {"first"};
        Supplier<Object> supplier = () -> valueHolder[0];

        AssociationValue associationValue = new AssociationValue(retriever, operator, supplier);
        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn("first");

        Predicate<EventMessage> predicate = associationValue.asEventMessagePredicate(converter);

        assertTrue(predicate.test(eventMessage), "Should match 'first'");

        valueHolder[0] = "second";
        assertFalse(predicate.test(eventMessage), "Should not match 'first' anymore");

        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn("second");
        assertTrue(predicate.test(eventMessage), "Should match 'second'");
    }
}
