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

package org.axonframework.messaging.core;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * An {@link ApplicationContext} implementation that does not provide any components.
 * <p>
 * It is useful as a placeholder in tests, but you should never use it if you want to be able to retrieve components
 * from the {@link ProcessingContext} in your components.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
@Internal
public class EmptyApplicationContext implements ApplicationContext {

    /**
     * Returns the singleton instance of the empty application context.
     */
    public static final EmptyApplicationContext INSTANCE = new EmptyApplicationContext();

    private EmptyApplicationContext() {
    }

    @Override
    public <C> C component(Class<C> type, @Nullable String name) {
        throw new UnsupportedOperationException(
                "EmptyApplicationContext does not provide any components. " +
                        "You should never use it if you want to be able to retrieve components from the ProcessingContext."
        );
    }
}
