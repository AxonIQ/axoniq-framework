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
package org.axonframework.messaging.queryhandling.annotation;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.MessageHandler;
import org.axonframework.messaging.queryhandling.QueryMessage;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to be placed on methods that can handle {@link QueryMessage queries}, thus making them
 * {@link org.axonframework.messaging.queryhandling.QueryHandler QueryHandlers}.
 * <p>
 * Query handler annotated methods are typically subscribed with a
 * {@link org.axonframework.messaging.queryhandling.QueryBus} as part of an {@link AnnotatedQueryHandlingComponent}.
 * <p>
 * The parameters of the annotated method are resolved using parameter resolvers. Axon provides a number of parameter
 * resolvers that allow you to use the following parameter types:<ul>
 * <li>The first parameter is always the {@link QueryMessage#payload() payload} of the {@code QueryMessage}.
 * <li>Parameters annotated with {@link org.axonframework.messaging.core.annotation.MetadataValue} will resolve to the
 * {@link org.axonframework.messaging.core.Metadata} value with the key as indicated on the annotation. If required is
 * false (default), null is passed when the metadata value is not present. If required is true, the resolver will not
 * match and prevent the method from being invoked when the metadata value is not present.</li>
 * <li>Parameters of type {@code Metadata} will have the entire {@link QueryMessage#metadata() query message metadata}
 * injected.</li>
 * <li>Parameters assignable to {@link org.axonframework.messaging.core.Message} will have the entire {@link
 * QueryMessage} injected (if the message is assignable to that parameter). If the first parameter is of type message,
 * it effectively matches a query of any type. Due to type erasure, Axon cannot detect what parameter is expected. In
 * such case, it is best to declare a parameter of the payload type, followed by a parameter of type
 * {@code Message}.</li>
 * <li>A parameter of type {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} will inject the active
 * processing context at that moment in time.</li>
 * </ul>
 *
 * @author Marc Gathier
 * @since 3.1.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@MessageHandler(messageType = QueryMessage.class)
public @interface QueryHandler {

    /**
     * The name of the query this handler listens to, matching to the {@link MessageType#name()} from the
     * {@link QueryMessage#type()}.
     * <p>
     * When not defined the {@link org.axonframework.messaging.core.MessageTypeResolver} will derive the name based on
     * the payload type (thus the <b>first</b> parameter) of the annotated method.
     *
     * @return The name of the query this handler listens to, matching to the {@link MessageType#name()} from the
     * {@link QueryMessage#type()}.
     */
    String queryName() default "";
}
