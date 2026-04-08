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

package org.axonframework.modelling.command;

import org.axonframework.messaging.commandhandling.annotation.RoutingKey;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Field or method level annotation that marks a field or method providing the identifier of the aggregate that a
 * command targets.
 * <p/>
 * If placed on a method, that method must contain no parameters. The return value will be used as the
 * Aggregate Identifier.
 * <p/>
 * If placed on a field, the field's value will be converted into an AggregateIdentifier instance identical to how a
 * method's return value is converted.
 *
 * @author Allard Buijze
 * @since 1.2
 */
@RoutingKey
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TargetAggregateIdentifier {

}
