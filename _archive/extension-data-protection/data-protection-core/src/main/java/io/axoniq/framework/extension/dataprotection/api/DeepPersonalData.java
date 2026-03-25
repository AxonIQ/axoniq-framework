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
package io.axoniq.framework.extension.dataprotection.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field annotation that marks the field as containing an object that contains personal data. This triggers the
 * Axon Data Protection Module to process the value held in this field, examining it for annotations and encrypting
 * accordingly. The annotation can also be used as a meta-annotation on some other application-specific annotation.
 * <p>
 * Please see the Axon Data Protection Module guide for a detailed discussion of the usage of this annotation.
 * <p>
 * For use in Scala programs, there is a <code>deepPersonalData</code> type alias for this annotation, defined
 * in the package object of the api package.
 * This alias has the <code>scala.annotation.meta.field</code> meta-annotation which allows it to be directly
 * used on Scala class parameters, including case classes.
 *
 * @author Frans van Buul
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.FIELD})
@Repeatable(DeepPersonalDataContainer.class)
public @interface DeepPersonalData {

    /**
     * Get the {@link Scope} of the annotation. For non-{@link java.util.Map} fields, this should always be
     * <code>DEFAULT</code>, whereas for {@link java.util.Map} fields it should always be <code>KEY</code>,
     * <code>VALUE</code>, or <code>BOTH</code>.
     *
     * @return the scope
     */
    Scope scope() default Scope.DEFAULT;
}
