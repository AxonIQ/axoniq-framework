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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field or method level annotation that marks a field or method providing the routing key that a command targets.
 * <p/>
 * If placed on a method, that method must contain no parameters. The return value will be used as the Routing Key.
 * <p/>
 * If placed on a field, the field's value will be converted into a Routing Key instance identical to how a method's
 * return value is converted.
 *
 * @author Steven van Beelen
 * @since 4.0
 * @deprecated Use attributes of {@link @CommandHandler} annotation to specify the routing key instead.
 */
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Deprecated(since = "5.0.0")
public @interface RoutingKey {

}
