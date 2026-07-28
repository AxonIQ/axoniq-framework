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

import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
import io.micrometer.tracing.propagation.Propagator;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Metadata;
import org.jspecify.annotations.Nullable;

/**
 * {@link Propagator.Getter} implementation that extracts a propagated trace context from a message's
 * {@link Metadata}.
 * <p>
 * The trace parent is part of the message metadata when it was set on dispatch by {@link MetadataPropagatorSetter}
 * (through {@link org.axonframework.messaging.tracing.Span#propagateContext(org.axonframework.messaging.core.Message)}).
 * {@link MicrometerSpanFactory} uses this getter to reconstruct the remote parent when creating handler spans, so
 * cross-process parenting works.
 * <p>
 * This type is {@link Internal} because it is an implementation detail of the Micrometer Tracing binding; it is
 * exposed only so {@link MicrometerSpanFactory} and tests can reference the shared {@link #INSTANCE}.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@Internal
public final class MetadataPropagatorGetter implements Propagator.Getter<Metadata> {

    /**
     * Shared singleton instance, used by {@link MicrometerSpanFactory}.
     */
    public static final MetadataPropagatorGetter INSTANCE = new MetadataPropagatorGetter();

    private MetadataPropagatorGetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public @Nullable String get(Metadata carrier, String key) {
        return carrier.get(key);
    }
}
