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

package org.axonframework.extension.spring.authorization;

import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.annotation.Secured;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * A {@link MessageHandlerInterceptor} that verifies authorization based on {@link Secured} annotations on the payload
 * of {@link Message Messages}.
 *
 * @param <T> The message type this interceptor can process
 * @author Roald Bankras
 * @since 4.11.0
 */
public class MessageAuthorizationHandlerInterceptor<M extends Message> implements MessageHandlerInterceptor<M> {

    /**
     * Metadata key for authorities.
     */
    public static final String METADATA_AUTHORITIES_KEY = "authorities";
    private static final Logger logger = LoggerFactory.getLogger(MessageAuthorizationHandlerInterceptor.class);

    @Override
    public MessageStream<?> interceptOnHandle(M message,
                                              ProcessingContext context,
                                              MessageHandlerInterceptorChain<M> interceptorChain) {
        if (!AnnotationUtils.isAnnotationPresent(message.payloadType(), Secured.class)) {
            return interceptorChain.proceed(message, context);
        }
        Secured annotation = message.payloadType().getAnnotation(Secured.class);
        Set<String> requiredAuthorities = Arrays.stream(annotation.value()).collect(Collectors.toSet());
        try {
            Set<String> messageAuthorities =
                    Optional.ofNullable(message.metadata().get(METADATA_AUTHORITIES_KEY))
                            .map(authorityMetadata -> {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Found authorities [{}]", authorityMetadata);
                                }
                                return new HashSet<>(Arrays.asList(authorityMetadata.split(",")));
                            })
                            .orElseThrow(() -> new UnauthorizedMessageException(
                                    "No authorities found for message with identifier [" + message.identifier() + "]"
                            ));

            if (logger.isDebugEnabled()) {
                logger.debug("Authorizing for [{}] and [{}]", message.type().name(), annotation.value());
            }

            messageAuthorities.retainAll(requiredAuthorities);
            if (!messageAuthorities.isEmpty()) {
                return interceptorChain.proceed(message, context);
            }
            throw new UnauthorizedMessageException(
                    "Unauthorized message with identifier [" + message.identifier() + "]"
            );
        } catch (UnauthorizedMessageException e) {
            return MessageStream.failed(new UnauthorizedMessageException(
                    "Unauthorized message with identifier [" + message.identifier() + "]"
            ));
        }
    }
}

