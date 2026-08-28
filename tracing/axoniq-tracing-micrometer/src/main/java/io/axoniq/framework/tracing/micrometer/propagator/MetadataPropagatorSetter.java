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

package io.axoniq.framework.tracing.micrometer.propagator;

import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.annotation.Internal;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * {@link Propagator.Setter} implementation that writes the active trace context into a mutable {@link Map}.
 * <p>
 * Since an {@link org.axonframework.messaging.core.Message} is immutable, this setter cannot mutate the message
 * directly. Instead it writes the propagation entries into a temporary {@code Map}, which the binding's span
 * implementation then merges onto the message via
 * {@link org.axonframework.messaging.core.Message#andMetadata(Map)}.
 * <p>
 * This type is {@link Internal} because it is an implementation detail of the Micrometer Tracing binding; it is
 * exposed only so the binding and tests can reference the shared {@link #INSTANCE}.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
public final class MetadataPropagatorSetter implements Propagator.Setter<Map<String, String>> {

    /**
     * Shared singleton instance used by the Micrometer binding.
     */
    public static final MetadataPropagatorSetter INSTANCE = new MetadataPropagatorSetter();

    private MetadataPropagatorSetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public void set(@Nullable Map<String, String> carrier, String key, String value) {
        if (carrier == null) {
            throw new IllegalArgumentException("The provided carrier may not be null!");
        }
        carrier.put(key, value);
    }
}
