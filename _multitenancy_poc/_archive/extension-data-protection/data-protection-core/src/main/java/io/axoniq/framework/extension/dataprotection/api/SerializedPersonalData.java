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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field annotation that marks the field as containing personal data. This triggers the Axon Data Protection Module
 * to encrypt this fields when it processes an instance of the class declaring the field. The annotation
 * can also be used as a meta-annotation on some other application-specific annotation.
 * <p>
 * For use in Scala programs, there is a <code>serializedPersonalData</code> type alias for this annotation, defined
 * in the package object of the api package.
 * This alias has the <code>scala.annotation.meta.field</code> meta-annotation which allows it to be directly
 * used on Scala class parameters, including case classes.
 *
 * @author Frans van Buul
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.FIELD})
public @interface SerializedPersonalData {

    /**
     * Get the name of the group of personal data fields that this field belong to. This is useful if multiple
     * keys are used on a single class. The group links the field to a particular key, since the {@link DataSubjectId}
     * annotation has this same field.
     * <p>
     * Optional. If left empty this defaults to the default group, identified by the empty string.
     *
     * @return the group
     */
    String group() default "";

    /**
     * Defines which field will be used to store the encrypted data in.
     * <p>
     * Optional. If left empty this default to the empty string, which will cause the Axon Data Protection Module to
     * look for a field with the same name as the primary field, suffixed by "Encrypted".
     *
     * @return the storage field
     */
    String storageField() default "";

    /**
     * Defines a string to be used by the {@link ReplacementValueProvider} to determine the behaviour in
     * case the decryption key is unavailable. By default, the value of this attributed will be ignored for
     * {@link SerializedPersonalData}.
     *
     * @return the replacement
     */
    String replacement() default "";
}
