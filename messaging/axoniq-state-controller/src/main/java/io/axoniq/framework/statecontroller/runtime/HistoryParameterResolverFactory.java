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

package io.axoniq.framework.statecontroller.runtime;

import io.axoniq.framework.statecontroller.History;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.concurrent.CompletableFuture;

/**
 * {@link ParameterResolverFactory} that injects the unbound root {@link History} into handler method parameters
 * of that type — one half of the signature (together with an
 * {@link io.axoniq.framework.statecontroller.Outcome Outcome} return type) that opts a plain
 * {@code @CommandHandler} method into the State Controller.
 * <p>
 * Discovered by AF5 via {@link java.util.ServiceLoader ServiceLoader} (registered through
 * {@code META-INF/services/org.axonframework.messaging.core.annotation.ParameterResolverFactory}). On each
 * handler invocation, the resolver delegates to {@link OutcomeDispatch#sessionFor(ProcessingContext)} so the
 * injected root shares the loading session — and therefore the batched reads and the DCB consistency marker —
 * with any {@link io.axoniq.framework.statecontroller.decisions.DecisionContext DecisionContext} resolved for the
 * same {@link ProcessingContext}.
 * <p>
 * Marked {@link Internal @Internal} because it is loaded by AF5's ServiceLoader and is not a designed extension
 * point; user code should not subclass it.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public final class HistoryParameterResolverFactory
        implements ParameterResolverFactory, ParameterResolver<History> {

    @Override
    public @Nullable ParameterResolver<?> createInstance(Executable executable,
                                                         Parameter[] parameters,
                                                         int parameterIndex) {
        Class<?> type = parameters[parameterIndex].getType();
        if (!History.class.isAssignableFrom(type)) {
            return null;
        }
        return this;
    }

    @Override
    public CompletableFuture<History> resolveParameterValue(ProcessingContext context) {
        return CompletableFuture.completedFuture(OutcomeDispatch.sessionFor(context).history());
    }

    @Override
    public boolean matches(ProcessingContext context) {
        return true;
    }
}
