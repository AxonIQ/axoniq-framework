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

package org.axonframework.messaging.commandhandling.annotation;

import org.axonframework.messaging.core.annotation.AggregateType;
import org.junit.jupiter.api.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Optional;

import static org.axonframework.common.annotation.AnnotationUtils.findAnnotationAttributes;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link org.axonframework.common.annotation.AnnotationUtils}.
 *
 * @author Simon Zambrovski
 */
class AnnotationUtilsTest {

    @Test
    void findAttributesOnNonExistentAnnotation() throws NoSuchMethodException {
        Optional<Map<String, Object>> result =
                findAnnotationAttributes(getClass().getDeclaredMethod("dynamicallyOverridden"), AggregateType.class);
        assertFalse(result.isPresent(), "Didn't expect attributes to be found for non-existent annotation");
    }

    @DynamicOverrideAnnotated(property = "dynamic-override")
    public void dynamicallyOverridden() {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
    @TheTarget
    public @interface DynamicOverrideAnnotated {

        String property();

        String extraValue() default "extra";

        String theTarget() default "otherValue";
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
    public @interface TheTarget {

        String property() default "value";

        String value() default "value()";
    }
}
