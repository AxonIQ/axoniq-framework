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
package io.axoniq.framework.dataprotection.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field annotation that marks the field as containing an identifier of the key to be used when
 * encrypting personal data fields of this class. It can also be used as a meta-annotation on
 * some other application-specific annotation.
 * <p>
 * Multiple annotations of this type may be present on the same field, but they should have
 * different group attributes.
 * <p>
 * For use in Scala programs, there is a <code>dataSubjectId</code> type alias for this annotation, defined
 * in the package object of the api package.
 * This alias has the <code>scala.annotation.meta.field</code> meta-annotation which allows it to be directly
 * used on Scala class parameters, including case classes.
 *
 * @author Frans van Buul
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.FIELD})
@Repeatable(DataSubjectIdContainer.class)
public @interface DataSubjectId {

    /**
     * Get the name of the group of personal data fields that field identifies the key for. This is useful if multiple
     * keys are used on a single class. The group links key identifier to fields, since the {@link PersonalData}
     * annotation has this same field. If multiple annotations of this type occur in the same class, all group attributes
     * must be unique.
     * <p>
     * Optional. If left empty this defaults to the default group, identified by the empty string.
     *
     * @return the group
     */
    String group() default "";

    /**
     * Get the prefix used when constructing key ids from the field value. The effective key id will be
     * prefix + value.
     * <p>
     * Optional. If left empty this defaults to the empty string.
     *
     * @return the prefix
     */
    String prefix() default "";

    /**
     * Get the {@link Scope} of the annotation. For non-{@link java.util.Map} fields, this should always be
     * <code>DEFAULT</code>, whereas for {@link java.util.Map} fields it should always be <code>KEY</code>.
     *
     * @return the scope
     */
    Scope scope() default Scope.DEFAULT;
}
