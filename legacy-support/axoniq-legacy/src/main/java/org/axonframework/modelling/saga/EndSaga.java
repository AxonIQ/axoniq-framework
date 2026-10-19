/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga;

import org.axonframework.messaging.core.annotation.HasHandlerAttributes;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the annotated {@link SagaEventHandler} method indicates the end of a Saga instance's lifecycle. When
 * event handling completes, the Saga is destroyed and may no longer receive events.
 * <p/>
 * This annotation can only appear on methods that have been annotated with {@link SagaEventHandler}.
 *
 * @author Allard Buijze
 * @since 0.7
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@HasHandlerAttributes
public @interface EndSaga {

}
