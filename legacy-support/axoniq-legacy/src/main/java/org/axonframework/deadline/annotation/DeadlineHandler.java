/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.deadline.annotation;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.core.annotation.MessageHandler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to mark handlers which are capable of handling a {@link DeadlineMessage}. It is a specialization of
 * {@link MessageHandler} where the {@code messageType} is set to {@link DeadlineMessage}. Hence, any parameter
 * injection that works for event handlers works for deadline handlers as well.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @see MessageHandler
 * @since 3.3.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@MessageHandler(messageType = DeadlineMessage.class)
public @interface DeadlineHandler {

    /**
     * The name of the deadline this handler listens to. Defaults to an empty {@link String}, meaning the handler
     * accepts deadlines of any name. A handler declaring a specific name takes precedence over one accepting any name.
     *
     * @return the name of the deadline this handler listens to, or an empty {@link String} to accept any deadline
     */
    String deadlineName() default "";

    /**
     * Specifies the type of message payload that can be handled by the member method. The payload of the message should
     * be assignable to this type. Defaults to any {@link Object}.
     *
     * @return the payload type handled by the function annotated with {@code @DeadlineHandler}
     */
    Class<?> payloadType() default Object.class;
}
