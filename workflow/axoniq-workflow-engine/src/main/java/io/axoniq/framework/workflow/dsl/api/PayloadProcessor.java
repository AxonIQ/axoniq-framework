/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Action executed consuming payload and returning payload as a result run in a provided processing context.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface PayloadProcessor
        extends BiFunction<ProcessingContext, Map<String, @Nullable Object>, Map<String, @Nullable Object>> {

}
