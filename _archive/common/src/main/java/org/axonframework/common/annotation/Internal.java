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

package org.axonframework.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used to mark Axon Framework internal code. Internal code may undergo breaking changes if our
 * development cycle requires it so.
 * <p>
 * Code marked with this annotation is still usable by <b>everybody</b>. Just be aware unexpected changes may occur on
 * internal code. Hence, Axon Framework internal code may introduce breaking changes within minor and patch releases.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE})
public @interface Internal {

}
