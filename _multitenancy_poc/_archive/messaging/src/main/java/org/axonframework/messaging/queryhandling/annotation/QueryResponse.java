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
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.annotation.Message;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to mark an object as a query response.
 * <p>
 * Allows for specifying the business/domain {@link #name()} of the query and the {@link #version()} of the query
 * response. The fields are used to map an annotated-query to a
 * {@link QueryResponseMessage}.
 *
 * @author Simon Zambrovski
 * @since 5.0.0
 */
@Message(messageType = QueryResponseMessage.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
public @interface QueryResponse {
    /**
     * The namespace or (bounded) context of the query response.
     * <p>
     * Is used to define the {@link QualifiedName#namespace()} of a fully qualified name.
     * <p>
     * Defaults to the package name of the annotated class.
     *
     * @return The namespace or (bounded) context of the query response.
     */
    String namespace() default "";

    /**
     * The business or domain name of the query response.
     * <p>
     * Is used to define the {@link QualifiedName#localName()} of a fully qualified name.
     * <p>
     * Defaults to the simple name of the annotated class. Note that when an inner class is annotated, the simple name
     * does not include the names of the enclosing classes.
     *
     * @return The business or domain name of the query response.
     */
    String name() default "";

    /**
     * The version of the query response.
     * <p>
     * Will typically be mapped to the {@link MessageType#version()}. Defaults to {@link MessageType#DEFAULT_VERSION}.
     *
     * @return The version of the query response.
     */
    String version() default MessageType.DEFAULT_VERSION;

}
