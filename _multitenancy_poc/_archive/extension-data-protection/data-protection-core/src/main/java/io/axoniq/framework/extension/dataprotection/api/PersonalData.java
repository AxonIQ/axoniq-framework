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
 * Field annotation that marks the field as containing personal data. This triggers the Axon Data Protection Module
 * to encrypt this fields when it processes an instance of the class declaring the field. The annotation
 * can also be used as a meta-annotation on some other application-specific annotation.
 * <p>
 * For use in Scala programs, there is a <code>personalData</code> type alias for this annotation, defined
 * in the package object of the api package.
 * This alias has the <code>scala.annotation.meta.field</code> meta-annotation which allows it to be directly
 * used on Scala class parameters, including case classes.
 *
 * @author Frans van Buul
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.FIELD})
@Repeatable(PersonalDataContainer.class)
public @interface PersonalData {

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
     * Get the {@link Scope} of the annotation. For non-{@link java.util.Map} fields, this should always be
     * <code>DEFAULT</code>, whereas for {@link java.util.Map} fields it should always be <code>KEY</code>,
     * <code>VALUE</code>, or <code>BOTH</code>.
     *
     * @return the scope
     */
    Scope scope() default Scope.DEFAULT;

    /**
     * Defines a string to be used by the {@link ReplacementValueProvider} to determine the behaviour in
     * case the decryption key is unavailable. By default, the value of this annotation will be used in
     * literal form for <code>String</code> fields and will be ignored for <code>byte[]</code>, but custom implementations of
     * {@link ReplacementValueProvider} may do otherwise.
     *
     * @return the replacement
     */
    String replacement() default "";

    /**
     * Determines whether we should perform encryption if a field is already encrypted. By default this is false,
     * resulting in idempotent behaviour of the {@link FieldEncrypter#encrypt(Object)} method. In some cases, it
     * is desirable to make this true to deliberately perform 'double-encryption'.
     *
     * @return the reencrypt value
     */
    boolean reencrypt() default false;

}

