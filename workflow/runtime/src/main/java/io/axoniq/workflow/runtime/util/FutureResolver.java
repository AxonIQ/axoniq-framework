/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.util;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;

/**
 * Resolves a {@link CompletableFuture} according to a configurable waiting policy.
 * <p>
 * Applications can provide one implementation through Java's {@link ServiceLoader} mechanism. The selected resolver is
 * registered as a configuration component and is used wherever workflow code waits for a future. When no service is
 * present, the default resolver waits infinitely without a deadline.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public abstract class FutureResolver {

    private static final Logger logger = LoggerFactory.getLogger(FutureResolver.class);
    private static final FutureResolver INSTANCE = locateResolver();

    private static FutureResolver locateResolver() {
        var resolver = findResolver(Thread.currentThread().getContextClassLoader(), "context");
        if (resolver == null) {
            resolver = findResolver(FutureResolver.class.getClassLoader(), "FutureResolver");
        }
        if (resolver == null) {
            logger.debug("Using the default future resolver.");
            return new DefaultFutureResolver();
        }
        logger.info("Found custom future resolver: {}", resolver.getClass().getName());
        return resolver;
    }

    private static FutureResolver findResolver(ClassLoader classLoader, String classLoaderName) {
        Iterator<FutureResolver> resolvers = ServiceLoader.load(FutureResolver.class, classLoader).iterator();
        if (!resolvers.hasNext()) {
            return null;
        }
        var resolver = resolvers.next();
        if (resolvers.hasNext()) {
            logger.warn("More than one FutureResolver was found using the {} class loader. "
                                + "The first resolver is used.", classLoaderName);
        }
        return resolver;
    }

    /**
     * Returns the resolver selected through service loading.
     *
     * @return the configured resolver, or the default resolver when no service is present
     */
    @Nonnull
    public static FutureResolver getInstance() {
        return INSTANCE;
    }

    /**
     * Resolves a future through the resolver registered in the processing context.
     * <p>
     * The service-loaded resolver is used as a fallback for contexts created in tests or outside configured workflow
     * processing.
     *
     * @param processingContext context containing the configured resolver
     * @param future            future to resolve
     */
    public static void resolve(@Nonnull ProcessingContext processingContext,
                               @Nonnull CompletableFuture<?> future) {
        Objects.requireNonNull(processingContext, "Processing context must not be null");
        Objects.requireNonNull(future, "Future must not be null");
        var resolver = processingContext.component(FutureResolver.class);
        (resolver != null ? resolver : getInstance()).resolve(future);
    }

    /**
     * Resolves the given future according to this resolver's policy.
     *
     * @param future future to resolve
     */
    public abstract void resolve(@Nonnull CompletableFuture<?> future);
}
