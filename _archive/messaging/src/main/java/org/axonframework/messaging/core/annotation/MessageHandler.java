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

package org.axonframework.messaging.core.annotation;

import org.axonframework.messaging.core.Message;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation indicating that a member method should be able to respond to {@link Message}s.
 * <p>
 * It is not recommended to put this annotation on methods or constructors directly. Instead, put this annotation on
 * another annotation that expresses the type of message handled.
 *
 * @author Allard Buijze
 * @since 3.0.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@HasHandlerAttributes
public @interface MessageHandler {

    /**
     * Specifies the type of message that can be handled by the member method. Defaults to any {@link Message}.
     *
     * @return The type of {@link Message} handled by the function annotated with {@code @MessageHandler}.
     */
    Class<? extends Message> messageType() default Message.class;

    /**
     * Specifies the type of message payload that can be handled by the member method. The payload of the message should
     * be assignable to this type. Defaults to any {@link Object}.
     *
     * @return The payload type handled by the function annotated with {@code @MessageHandler}.
     */
    Class<?> payloadType() default Object.class;
}
