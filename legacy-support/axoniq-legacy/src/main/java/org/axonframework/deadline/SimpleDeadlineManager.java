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

package org.axonframework.deadline;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.ClockUtils;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Implementation of {@link DeadlineManager} which uses Java's {@link ScheduledExecutorService} as scheduling and
 * triggering mechanism.
 * <p>
 * Note that this mechanism is non-persistent. Scheduled tasks will be lost when the JVM is shut down, unless special
 * measures have been taken to prevent that. For more flexible and powerful scheduling options, see
 * {@link org.axonframework.deadline.quartz.QuartzDeadlineManager}.
 * <p>
 * A fired deadline runs in a unit of work from the configured {@link UnitOfWorkFactory}, with the registered handler
 * interceptors around its delivery to the {@link org.axonframework.messaging.ScopeAware} components of the
 * {@link ScopeAwareProvider}. A failing delivery is logged, as the deadline cannot be retried.
 * <pre>{@code
 * SimpleDeadlineManager deadlineManager =
 *         SimpleDeadlineManager.builder()
 *                              .scopeAwareProvider(scopeAwareProvider)
 *                              .unitOfWorkFactory(configuration.getComponent(UnitOfWorkFactory.class))
 *                              .build();
 * }</pre>
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @author Jakob Hatzl
 * @since 3.3
 */
public class SimpleDeadlineManager extends AbstractDeadlineManager {

    private static final Logger logger = LoggerFactory.getLogger(SimpleDeadlineManager.class);
    private static final String THREAD_FACTORY_GROUP_NAME = "deadlineManager";

    private final ScheduledExecutorService scheduledExecutorService;
    private final DeadlineDelivery delivery;

    private final Map<DeadlineId, Future<?>> scheduledTasks = new ConcurrentHashMap<>();

    /**
     * Instantiate a Builder to be able to create a {@code SimpleDeadlineManager}.
     * <p>
     * The {@link ScheduledExecutorService} is defaulted to an {@link Executors#newSingleThreadScheduledExecutor()}
     * which contains an {@link AxonThreadFactory}. The {@link ScopeAwareProvider} and the {@link UnitOfWorkFactory} are
     * <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@code SimpleDeadlineManager}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Instantiate a {@code SimpleDeadlineManager} based on the fields contained in the {@link Builder} to handle the
     * process around scheduling and triggering a {@link DeadlineMessage}.
     * <p>
     * Will assert that the {@link ScopeAwareProvider}, {@link ScheduledExecutorService} and {@link UnitOfWorkFactory}
     * are not {@code null}, and will throw an {@link AxonConfigurationException} if either of them is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@code SimpleDeadlineManager} instance
     */
    protected SimpleDeadlineManager(Builder builder) {
        builder.validate();
        this.scheduledExecutorService = builder.scheduledExecutorService;
        this.delivery = new DeadlineDelivery(builder.unitOfWorkFactory,
                                             builder.scopeAwareProvider,
                                             handlerInterceptors());
    }

    @Override
    public String schedule(Instant triggerDateTime,
                           String deadlineName,
                           @Nullable Object messageOrPayload,
                           ScopeDescriptor deadlineScope) {
        DeadlineMessage deadlineMessage = asDeadlineMessage(deadlineName, messageOrPayload, triggerDateTime);
        String deadlineMessageId = deadlineMessage.identifier();
        DeadlineId deadlineId = new DeadlineId(deadlineName, deadlineScope, deadlineMessageId);
        runOnPrepareCommitOrNow(context -> {
            DeadlineMessage interceptedDeadlineMessage = processDispatchInterceptors(deadlineMessage, context);
            DeadlineTask deadlineTask = new DeadlineTask(deadlineId, interceptedDeadlineMessage);
            Duration triggerDuration = Duration.between(ClockUtils.instant(), triggerDateTime);
            Future<?> scheduledFuture = scheduledExecutorService.schedule(
                    deadlineTask,
                    triggerDuration.toMillis(),
                    TimeUnit.MILLISECONDS
            );
            scheduledTasks.put(deadlineId, scheduledFuture);
        });
        return deadlineMessageId;
    }

    @Override
    public void cancelSchedule(String deadlineName, String scheduleId) {
        runOnPrepareCommitOrNow(
                context -> scheduledTasks.keySet().stream()
                                         .filter(scheduledTaskId -> scheduledTaskId.deadlineName().equals(deadlineName)
                                                 && scheduledTaskId.deadlineId().equals(scheduleId))
                                         .forEach(this::cancelSchedule)
        );
    }

    @Override
    public void cancelAll(String deadlineName) {
        runOnPrepareCommitOrNow(
                context -> scheduledTasks.keySet().stream()
                                         .filter(scheduledTaskId -> scheduledTaskId.deadlineName().equals(deadlineName))
                                         .forEach(this::cancelSchedule)
        );
    }

    @Override
    public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
        runOnPrepareCommitOrNow(
                context -> scheduledTasks.keySet().stream()
                                         .filter(scheduledTaskId -> scheduledTaskId.deadlineName().equals(deadlineName)
                                                 && scheduledTaskId.deadlineScope().equals(scope))
                                         .forEach(this::cancelSchedule)
        );
    }

    private void cancelSchedule(DeadlineId deadlineId) {
        Future<?> future = scheduledTasks.remove(deadlineId);
        if (future != null) {
            future.cancel(false);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Shuts down the {@link ScheduledExecutorService}, which ignores every further call.
     */
    @Override
    public void shutdown() {
        scheduledExecutorService.shutdown();
    }

    private record DeadlineId(String deadlineName, ScopeDescriptor deadlineScope, String deadlineId) {

    }

    /**
     * Builder class to instantiate a {@link SimpleDeadlineManager}.
     * <p>
     * The {@link ScheduledExecutorService} is defaulted to an {@link Executors#newSingleThreadScheduledExecutor()}
     * which contains an {@link AxonThreadFactory}. The {@link ScopeAwareProvider} and the {@link UnitOfWorkFactory} are
     * <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private @Nullable ScopeAwareProvider scopeAwareProvider;
        private ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new AxonThreadFactory(THREAD_FACTORY_GROUP_NAME));
        private @Nullable UnitOfWorkFactory unitOfWorkFactory;

        /**
         * Sets the {@link ScopeAwareProvider} which is capable of providing a stream of
         * {@link org.axonframework.messaging.Scope} instances for a given {@link ScopeDescriptor}. Used to return the
         * right Scope to trigger a deadline in.
         *
         * @param scopeAwareProvider a {@link ScopeAwareProvider} used to find the right
         *                           {@link org.axonframework.messaging.Scope} to trigger a deadline in
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder scopeAwareProvider(ScopeAwareProvider scopeAwareProvider) {
            assertNonNull(scopeAwareProvider, "ScopeAwareProvider may not be null");
            this.scopeAwareProvider = scopeAwareProvider;
            return this;
        }

        /**
         * Sets the {@link ScheduledExecutorService} used for scheduling and triggering deadlines. Defaults to a
         * {@link Executors#newSingleThreadScheduledExecutor()}, containing an {@link AxonThreadFactory}.
         *
         * @param scheduledExecutorService a {@link ScheduledExecutorService} used for scheduling and triggering
         *                                 deadlines
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
            assertNonNull(scheduledExecutorService, "ScheduledExecutorService may not be null");
            this.scheduledExecutorService = scheduledExecutorService;
            return this;
        }

        /**
         * Sets the {@link UnitOfWorkFactory} creating the unit of work a fired deadline runs in. Pass the factory of
         * the application's configuration, so that a fired deadline runs in the same kind of unit of work as other
         * messages: transactional if the factory is, and with a
         * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} that resolves components.
         *
         * @param unitOfWorkFactory the factory creating the unit of work a fired deadline runs in
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder unitOfWorkFactory(UnitOfWorkFactory unitOfWorkFactory) {
            assertNonNull(unitOfWorkFactory, "UnitOfWorkFactory may not be null");
            this.unitOfWorkFactory = unitOfWorkFactory;
            return this;
        }

        /**
         * Initializes a {@link SimpleDeadlineManager} as specified through this Builder.
         *
         * @return a {@link SimpleDeadlineManager} as specified through this Builder
         */
        public SimpleDeadlineManager build() {
            return new SimpleDeadlineManager(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(scopeAwareProvider, "The ScopeAwareProvider is a hard requirement and should be provided");
            assertNonNull(unitOfWorkFactory, "The UnitOfWorkFactory is a hard requirement and should be provided");
        }
    }

    private class DeadlineTask implements Runnable {

        private final DeadlineId deadlineId;
        private final DeadlineMessage deadlineMessage;

        private DeadlineTask(DeadlineId deadlineId, DeadlineMessage deadlineMessage) {
            this.deadlineMessage = deadlineMessage;
            this.deadlineId = deadlineId;
        }

        @Override
        public void run() {
            if (logger.isDebugEnabled()) {
                logger.debug("Triggered deadline");
            }
            try {
                Instant triggerInstant = ClockUtils.instant();
                delivery.deliver(new GenericDeadlineMessage(deadlineId.deadlineName(),
                                                            deadlineMessage,
                                                            () -> triggerInstant),
                                 deadlineId.deadlineScope());
            } catch (Exception e) {
                logger.error("An error occurred while triggering the deadline [{}] with identifier [{}]",
                             deadlineId.deadlineName(), deadlineId.deadlineId(), e);
            } finally {
                scheduledTasks.remove(deadlineId);
            }
        }
    }
}
