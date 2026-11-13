/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.common.annotation.Internal;

/**
 * Contract describing the cause of a workflow or step failure.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public interface Cause {

    /**
     * Fully-qualified class name of the originating {@link Throwable}.
     */
    String type();

    /**
     * Message describing the cause, typically {@link Throwable#getMessage()}.
     */
    String message();

    /**
     * Returns {@code true} when {@link #type()} matches the fully-qualified name of {@code clazz}. Use this instead of
     * {@code instanceof} when branching on the originating throwable's class — {@code instanceof} does not match
     * because the runtime cause is always {@link WorkflowExecutionException}, regardless of the originating type.
     */
    default boolean isType(Class<? extends Throwable> clazz) {
        return clazz.getName().equals(type());
    }
}
