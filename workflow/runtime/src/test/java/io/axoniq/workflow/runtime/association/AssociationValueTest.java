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
package io.axoniq.workflow.runtime.association;

import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
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
    private final EventConverter converter = Mockito.mock(EventConverter.class);
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

        assertThat(predicate.test(eventMessage)).isTrue();
        verify(retriever).apply(eventMessage, converter);
    }

    @Test
    void shouldNotMatchWhenValueIsDifferent() {
        String expectedValue = "123";
        AssociationValue associationValue = new AssociationValue(retriever, operator, () -> expectedValue);

        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn("456");

        Predicate<EventMessage> predicate = associationValue.asEventMessagePredicate(converter);

        assertThat(predicate.test(eventMessage)).isFalse();
    }

    @Test
    void shouldMatchWhenBothAreNull() {
        AssociationValue associationValue = new AssociationValue(retriever, operator, () -> null);

        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn(null);

        Predicate<EventMessage> predicate = associationValue.asEventMessagePredicate(converter);

        assertThat(predicate.test(eventMessage)).isTrue();
    }

    @Test
    void shouldEvaluateSupplierOnEachCall() {
        final String[] valueHolder = {"first"};
        Supplier<Object> supplier = () -> valueHolder[0];

        AssociationValue associationValue = new AssociationValue(retriever, operator, supplier);
        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn("first");

        Predicate<EventMessage> predicate = associationValue.asEventMessagePredicate(converter);

        assertThat(predicate.test(eventMessage)).withFailMessage("Should match 'first'").isTrue();

        valueHolder[0] = "second";
        assertThat(predicate.test(eventMessage)).withFailMessage("Should not match 'first' anymore").isFalse();

        when(retriever.apply(eq(eventMessage), eq(converter))).thenReturn("second");
        assertThat(predicate.test(eventMessage)).withFailMessage("Should match 'second'").isTrue();
    }
}
