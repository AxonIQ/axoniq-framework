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

import org.axonframework.messaging.core.annotation.HasHandlerAttributes;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Meta-Annotation used to demarcate {@link MessageHandlerInterceptor} annotated methods as interceptors that should
 * only act on the result of a handler invocation. This gives these handlers the opportunity to act on the result only,
 * without intercepting the call on the way <em>to</em> the handler.
 * <p>
 * The {@link #resultType()} can be used to limit the types of responses the handler should be invoked for.
 * <p>
 * This annotation is exclusively meant as a Meta-Annotation and cannot not be placed directly on a method.
 *
 * @author Allard Buijze
 * @see ExceptionHandler
 * @since 4.4.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE})
@HasHandlerAttributes
public @interface ResultHandler {

    /**
     * The type of result object that the annotated handler should be invoked for. The handler will be ignored if the
     * actual response type (regular or thrown exception) is not an instance of the type defined by this property, even
     * when the parameters of the method match the result.
     *
     * @return The type of result returned my functions annotated with {@link @ResultHandler}
     */
    Class<?> resultType() default Object.class;
}
