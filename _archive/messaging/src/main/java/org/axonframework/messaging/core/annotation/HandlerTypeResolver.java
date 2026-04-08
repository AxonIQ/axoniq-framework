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

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static org.axonframework.common.ReflectionUtils.methodsOf;
import static org.axonframework.common.annotation.AnnotationUtils.isAnnotationPresent;

/**
 * Uses reflection to know if a handler handles a certain type of messages.
 *
 * @author Gerard Klijs
 * @since 4.6.0
 */
public class HandlerTypeResolver {

    /**
     * Whether this handler has {@link CommandHandler} annotated methods.
     * @param handlerClass the class of the handler
     * @return whether it contains command handler methods
     */
    public static boolean isCommandHandler(Class<?> handlerClass){
        return isHandlerOfType(handlerClass, CommandHandler.class);
    }

    /**
     * Whether this handler has {@link EventHandler} annotated methods.
     * @param handlerClass the class of the handler
     * @return whether it contains event handler methods
     */
    public static boolean isEventHandler(Class<?> handlerClass){
        return isHandlerOfType(handlerClass, EventHandler.class);
    }

    /**
     * Whether this handler has {@link QueryHandler} annotated methods.
     * @param handlerClass the class of the handler
     * @return whether it contains query handler methods
     */
    public static boolean isQueryHandler(Class<?> handlerClass){
        return isHandlerOfType(handlerClass, QueryHandler.class);
    }

    private static boolean isHandlerOfType(Class<?> handlerClass, Class<? extends Annotation> annotationType){
        for (Method m: methodsOf(handlerClass)){
            if(isAnnotationPresent(m, annotationType)){
                return true;
            }
        }
        return false;
    }

    private HandlerTypeResolver() {
        // not to be instantiated
    }
}
