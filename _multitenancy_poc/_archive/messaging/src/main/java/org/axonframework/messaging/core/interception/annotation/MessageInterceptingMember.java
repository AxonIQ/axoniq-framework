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

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;

/**
 * Marker interface for {@link MessageHandlingMember} instances that need to be treated as interceptors, rather than
 * regular members.
 *
 * @param <T> The type that the handler was declared on.
 * @author Allard Buijze
 * @since 4.4.0
 */
@Internal
public interface MessageInterceptingMember<T> extends MessageHandlingMember<T> {

    @Override
    default int priority() {
        return 100_000;
    }
}
