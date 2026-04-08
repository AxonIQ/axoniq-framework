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

package org.axonframework.messaging.core.interception.annotation;

import org.axonframework.messaging.core.Message;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation marking a Handler as an interceptor for other handlers that is only interested in handling
 * exception results. This handler method will be invoked after a regular handler has been executed and may receive the
 * result of that handler as a parameter.
 * <p>
 * A handler will only be invoked when the parameters of this method match the combination of the handled Message and
 * the result of the handler method invocation.
 *
 * @author Allard Buijze
 * @since 4.4.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@MessageHandlerInterceptor
@ResultHandler(resultType = Exception.class)
public @interface ExceptionHandler {

    /**
     * Defines the type of result this handler needs to be triggered for. Defaults to all {@code Exception}s.
     */
    Class<? extends Exception> resultType() default Exception.class;

    /**
     * Specifies the type of message that can be handled by the member method. Defaults to any {@link Message}.
     */
    Class<? extends Message> messageType() default Message.class;

    /**
     * Specifies the type of message payload that can be handled by the member method. The payload of the message should
     * be assignable to this type. Defaults to any {@link Object}.
     */
    Class<?> payloadType() default Object.class;

}
