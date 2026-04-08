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

package org.axonframework.conversion;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.*;

/**
 * Test suite to validate {@link Converter} implementations.
 *
 * @author Steven van Beelen.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConverterTestSuite<C extends Converter> {

    protected C testSubject;

    @BeforeEach
    void setUp() {
        this.testSubject = buildConverter();
    }

    /**
     * Builds the {@link Converter} used in this test suite.
     *
     * @return The {@link Converter} used in this test suite.
     */
    protected abstract C buildConverter();

    @ParameterizedTest
    @MethodSource("sameTypeConversions")
    void shouldReturnSameInstanceIfSourceAndTargetTypeAreEqual(Object input, Type sourceAndTargetType) {
        Object result = testSubject.convert(input, sourceAndTargetType);
        assertThat(result).isSameAs(input);
    }

    private Stream<Arguments> sameTypeConversions() {
        return Stream.concat(commonSameTypeConversions(), specificSameTypeConversions());
    }

    /**
     * Returns the {@code Stream} of {@link Arguments} containing common same type conversion scenarios for <b>any</b>
     * {@link Converter} implementation.
     * <p>
     * Any {@link Arguments argument} consists out of one {@code Object} and one {@link Type}. The first parameter
     * refers to the {@code input} for the {@link Converter#convert(Object, Type)} operation, while the second parameter
     * is used as <b>both</b> the {@code sourceType} and {@code targetType}. Note that the {@code input} type is
     * expected to be identical to the given {@code Type}.
     * <p>
     * Can be overridden when needed for the {@code Converter} under tests.
     *
     * @return The {@code Stream} of {@link Arguments} containing same type conversion scenarios for <b>any</b>
     * {@link Converter} implementation.
     */
    protected Stream<Arguments> commonSameTypeConversions() {
        return Stream.of(
                arguments("Lorem Ipsum", String.class),
                arguments(42L, Long.class),
                arguments(new SomeInput("ID789", "SameType", 123), SomeInput.class),
                arguments(new SomeOtherInput("USR002", "No conversion"), SomeOtherInput.class)
        );
    }

    /**
     * Returns the {@code Stream} of {@link Arguments} containing same type conversion scenarios for <b>specific</b>
     * {@link Converter} implementations.
     * <p>
     * Any {@link Arguments argument} consists out of one {@code Object} and one {@link Type}. The first parameter
     * refers to the {@code input} for the {@link Converter#convert(Object, Type)} operation, while the second parameter
     * is used as <b>both</b> the {@code sourceType} and {@code targetType}. Note that the {@code input} type is
     * expected to be identical to the given {@code Type}.
     *
     * @return The {@code Stream} of {@link Arguments} containing same type conversion scenarios for <b>specific</b>
     * {@link Converter} implementations.
     */
    protected abstract Stream<Arguments> specificSameTypeConversions();

    @Test
    void convertForTargetTypeReturnsNullForNullInput() {
        Object result = testSubject.convert(null, Object.class);
        assertThat(result).isNull();

        result = testSubject.convert(null, Object.class);
        assertThat(result).isNull();
    }

    @Test
    void convertForSourceAndTargetTypeReturnsNullForNullInput() {
        Object result = testSubject.convert(null, Object.class);
        assertThat(result).isNull();
    }

    @ParameterizedTest
    @MethodSource("conversionScenarios")
    void convertForTargetTypeCanConvertBackToSource(Object input,
                                                    Type sourceType,
                                                    Type targetType) {
        Object targetConversion = testSubject.convert(input, targetType);
        Object sourceConversion = testSubject.convert(targetConversion, sourceType);
        assertThat(sourceConversion).isEqualTo(input);
    }

    @ParameterizedTest
    @MethodSource("conversionScenarios")
    void convertForSourceAndTargetTypeCanConvertBackToSource(Object input,
                                                             Type sourceType,
                                                             Type targetType) {
        Object targetConversion = testSubject.convert(input, targetType);
        Object sourceConversion = testSubject.convert(targetConversion, sourceType);
        assertThat(sourceConversion).isEqualTo(input);
    }

    private Stream<Arguments> conversionScenarios() {
        return Stream.concat(commonConversionScenarios(), specificConversionScenarios());
    }

    /**
     * Returns the {@code Stream} of {@link Arguments} containing common conversion scenarios for <b>any</b>
     * {@link Converter} implementation.
     * <p>
     * Any {@link Arguments argument} consists out of one {@code Object} and two {@link Type Types}. The first parameter
     * refers to the {@code input} for the {@link Converter#convert(Object, Type)} operation. The second and third
     * parameter refer to the {@code sourceType} and {@code targetType} respectively.
     * <p>
     * Can be overridden when needed for the {@code Converter} under tests.
     *
     * @return The {@code Stream} of {@link Arguments} containing common conversion scenarios for <b>any</b>
     * {@link Converter} implementation.
     */
    protected Stream<Arguments> commonConversionScenarios() {
        return Stream.of(
                arguments(new SomeInput("ID123", "TestName", 42), SomeInput.class, String.class),
                arguments(new SomeInput("ID456", "OtherName", 99), SomeInput.class, byte[].class),
                arguments(new SomeOtherInput("USR001", "Some description"), SomeOtherInput.class, String.class),
                arguments(new SomeOtherInput("USR002", "Another description"), SomeOtherInput.class, byte[].class),
                arguments("Lorem Ipsum", String.class, byte[].class),
                arguments("Lorem Ipsum".getBytes(StandardCharsets.UTF_8), byte[].class, InputStream.class)
        );
    }

    /**
     * Returns the {@code Stream} of {@link Arguments} containing conversion scenarios for <b>specific</b>
     * {@link Converter} implementations.
     * <p>
     * Any {@link Arguments argument} consists out of one {@code Object} and two {@link Type Types}. The first parameter
     * refers to the {@code input} for the {@link Converter#convert(Object, Type)} operation. The second and third
     * parameter refer to the {@code sourceType} and {@code targetType} respectively.
     *
     * @return The {@code Stream} of {@link Arguments} containing conversion scenarios for <b>specific</b>
     * {@link Converter} implementations.
     */
    protected abstract Stream<Arguments> specificConversionScenarios();

    public record SomeInput(String id, String name, int value) {

    }

    public record SomeOtherInput(String userId, String description) {

    }
}
