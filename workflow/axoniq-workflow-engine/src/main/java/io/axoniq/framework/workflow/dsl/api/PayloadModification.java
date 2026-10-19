/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.function.Function;

/**
 * Payload modification interface.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface PayloadModification extends Function<Map<String, @Nullable Object>, Map<String, @Nullable Object>> {

}
