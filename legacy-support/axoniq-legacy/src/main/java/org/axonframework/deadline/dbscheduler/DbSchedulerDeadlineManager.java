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

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.ScheduledExecution;
import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerState;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceNotFoundException;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskDescriptor;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.IdentifierFactory;
import org.axonframework.conversion.Converter;
import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.DeadlineDelivery;
import org.axonframework.deadline.DeadlineException;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.messaging.Scope;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.isNull;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineToken.TASK_NAME;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of {@link DeadlineManager} that delegates scheduling and triggering to a db-scheduler
 * {@link Scheduler}.
 * <p>
 * Each deadline is stored as an instance of the task named {@code AxonDeadline}, whose data is a
 * {@link DbSchedulerBinaryDeadlineDetails} or, without {@link Builder#useBinaryPojo(boolean) useBinaryPojo}, a
 * {@link DbSchedulerHumanReadableDeadlineDetails}. The scheduler has to know the matching task, from
 * {@link #binaryTask(Supplier)} or {@link #humanReadableTask(Supplier)}. The details keep the layout of Axon Framework
 * 4.13, so that tasks scheduled by Axon Framework 4 fire here, and tasks scheduled here fire on Axon Framework 4 nodes
 * sharing the scheduler's table. The deadline's payload, metadata and scope descriptor are converted with the
 * configured {@link Converter}, which has to match the serializer the Axon Framework 4 deadline manager used.
 * <p>
 * A fired deadline runs in a unit of work from the configured {@link UnitOfWorkFactory}, with the registered handler
 * interceptors around its delivery to the {@link org.axonframework.messaging.ScopeAware} components of the
 * {@link ScopeAwareProvider}. A failing delivery fails the task, which db-scheduler then retries.
 * <p>
 * Call {@link #start()} once the application is ready to handle deadlines, and {@link #shutdown()} when it stops,
 * unless the {@link Builder#startScheduler(boolean) startScheduler} and {@link Builder#stopScheduler(boolean)
 * stopScheduler} settings leave the scheduler's lifecycle to the application.
 * <pre>{@code
 * DbSchedulerDeadlineManager deadlineManager =
 *         DbSchedulerDeadlineManager.builder()
 *                                   .scheduler(scheduler)
 *                                   .scopeAwareProvider(scopeAwareProvider)
 *                                   .unitOfWorkFactory(configuration.getComponent(UnitOfWorkFactory.class))
 *                                   .converter(new JacksonConverter())
 *                                   .build();
 * }</pre>
 *
 * @author Gerard Klijs
 * @author Jakob Hatzl
 * @since 4.8.0
 */
@SuppressWarnings("Duplicates")
public class DbSchedulerDeadlineManager extends AbstractDeadlineManager {

    private static final Logger logger = getLogger(DbSchedulerDeadlineManager.class);
    private static final TaskDescriptor<DbSchedulerBinaryDeadlineDetails> binaryTaskDescriptor =
            TaskDescriptor.of(TASK_NAME, DbSchedulerBinaryDeadlineDetails.class);
    private static final TaskDescriptor<DbSchedulerHumanReadableDeadlineDetails> humanReadableTaskDescriptor =
            TaskDescriptor.of(TASK_NAME, DbSchedulerHumanReadableDeadlineDetails.class);

    private final Scheduler scheduler;
    private final StoredDeadlineConverter converter;
    private final DeadlineDelivery delivery;
    private final boolean useBinaryPojo;
    private final boolean startScheduler;
    private final boolean stopScheduler;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Instantiate a Builder to be able to create a {@code DbSchedulerDeadlineManager}.
     * <p>
     * The {@code useBinaryPojo}, {@code startScheduler} and {@code stopScheduler} settings are defaulted to
     * {@code true}. The {@link Scheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory} and {@link Converter}
     * are <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@code DbSchedulerDeadlineManager}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Instantiate a {@code DbSchedulerDeadlineManager} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link Scheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory} and
     * {@link Converter} are not {@code null}, and will throw an {@link AxonConfigurationException} if any of them is
     * {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@code DbSchedulerDeadlineManager} instance
     */
    protected DbSchedulerDeadlineManager(Builder builder) {
        builder.validate();
        this.scheduler = Objects.requireNonNull(builder.scheduler);
        this.converter = new StoredDeadlineConverter(Objects.requireNonNull(builder.converter));
        this.delivery = new DeadlineDelivery(Objects.requireNonNull(builder.unitOfWorkFactory),
                                             Objects.requireNonNull(builder.scopeAwareProvider),
                                             handlerInterceptors());
        this.useBinaryPojo = builder.useBinaryPojo;
        this.startScheduler = builder.startScheduler;
        this.stopScheduler = builder.stopScheduler;
    }

    @Override
    public String schedule(Instant triggerDateTime,
                           String deadlineName,
                           @Nullable Object messageOrPayload,
                           ScopeDescriptor deadlineScope) {
        DeadlineMessage deadlineMessage = asDeadlineMessage(deadlineName, messageOrPayload, triggerDateTime);
        String identifier = IdentifierFactory.getInstance().generateIdentifier();
        DbSchedulerDeadlineToken taskInstanceId = new DbSchedulerDeadlineToken(identifier);
        runOnPrepareCommitOrNow(context -> {
            DeadlineMessage message = processDispatchInterceptors(deadlineMessage, context);
            TaskInstance<?> taskInstance;
            if (useBinaryPojo) {
                taskInstance = binaryTask(deadlineName, deadlineScope, message, taskInstanceId);
            } else {
                taskInstance = humanReadableTask(deadlineName, deadlineScope, message, taskInstanceId);
            }
            scheduler.schedule(taskInstance, triggerDateTime);
            logger.debug("Task with id: [{}] was successfully created.", identifier);
        });
        return identifier;
    }

    private TaskInstance<?> binaryTask(String deadlineName,
                                       ScopeDescriptor deadlineScope,
                                       DeadlineMessage interceptedDeadlineMessage,
                                       DbSchedulerDeadlineToken taskInstanceId) {
        DbSchedulerBinaryDeadlineDetails details = DbSchedulerBinaryDeadlineDetails.serialized(
                deadlineName, deadlineScope, interceptedDeadlineMessage, converter
        );
        return binaryTaskDescriptor.instance(taskInstanceId.getId()).data(details).build();
    }

    private TaskInstance<?> humanReadableTask(String deadlineName,
                                              ScopeDescriptor deadlineScope,
                                              DeadlineMessage interceptedDeadlineMessage,
                                              DbSchedulerDeadlineToken taskInstanceId) {
        DbSchedulerHumanReadableDeadlineDetails details = DbSchedulerHumanReadableDeadlineDetails.serialized(
                deadlineName, deadlineScope, interceptedDeadlineMessage, converter
        );
        return humanReadableTaskDescriptor.instance(taskInstanceId.getId()).data(details).build();
    }

    /**
     * Gives the {@link Task} using {@link DbSchedulerBinaryDeadlineDetails} to execute a deadline via a
     * {@link Scheduler}. To be able to execute the task, this should be added to the task list, used to create the
     * scheduler.
     *
     * @param deadlineManagerSupplier a {@link Supplier} of a {@link DbSchedulerDeadlineManager}. Preferably a method
     *                                involving dependency injection is used. When those are not available the
     *                                {@link DbSchedulerDeadlineManagerSupplier} can be used instead.
     * @return a {@link Task} to execute a deadline
     */
    public static Task<DbSchedulerBinaryDeadlineDetails> binaryTask(
            Supplier<DbSchedulerDeadlineManager> deadlineManagerSupplier
    ) {
        return new Tasks.OneTimeTaskBuilder<>(TASK_NAME, DbSchedulerBinaryDeadlineDetails.class)
                .execute((taskInstance, context) -> {
                    DbSchedulerDeadlineManager deadlineManager = deadlineManagerSupplier.get();
                    if (isNull(deadlineManager)) {
                        throw new DeadlineManagerNotSuppliedException();
                    }
                    deadlineManager.execute(taskInstance.getData());
                });
    }

    /**
     * Gives the {@link Task} using {@link DbSchedulerHumanReadableDeadlineDetails} to execute a deadline via a
     * {@link Scheduler}. To be able to execute the task, this should be added to the task list, used to create the
     * scheduler.
     *
     * @param deadlineManagerSupplier a {@link Supplier} of a {@link DbSchedulerDeadlineManager}. Preferably a method
     *                                involving dependency injection is used. When those are not available the
     *                                {@link DbSchedulerDeadlineManagerSupplier} can be used instead.
     * @return a {@link Task} to execute a deadline
     */
    public static Task<DbSchedulerHumanReadableDeadlineDetails> humanReadableTask(
            Supplier<DbSchedulerDeadlineManager> deadlineManagerSupplier
    ) {
        return new Tasks.OneTimeTaskBuilder<>(TASK_NAME, DbSchedulerHumanReadableDeadlineDetails.class)
                .execute((taskInstance, context) -> {
                    DbSchedulerDeadlineManager deadlineManager = deadlineManagerSupplier.get();
                    if (isNull(deadlineManager)) {
                        throw new DeadlineManagerNotSuppliedException();
                    }
                    deadlineManager.execute(taskInstance.getData());
                });
    }

    @Override
    public void cancelSchedule(String deadlineName, String scheduleId) {
        runOnPrepareCommitOrNow(context -> {
            try {
                scheduler.cancel(new DbSchedulerDeadlineToken(scheduleId));
            } catch (TaskInstanceNotFoundException e) {
                logger.debug("Attempted to cancel task [{}] which does not exist. The task may have already been "
                                     + "canceled or the given schedule identifier is incorrect.", scheduleId);
            }
        });
    }

    @Override
    public void cancelAll(String deadlineName) {
        if (useBinaryPojo) {
            runOnPrepareCommitOrNow(context -> scheduler.fetchScheduledExecutionsForTask(
                    TASK_NAME,
                    DbSchedulerBinaryDeadlineDetails.class,
                    cancelIfBinaryDeadlineMatches(deadlineName)
            ));
        } else {
            runOnPrepareCommitOrNow(context -> scheduler.fetchScheduledExecutionsForTask(
                    TASK_NAME,
                    DbSchedulerHumanReadableDeadlineDetails.class,
                    cancelIfHumanReadableDeadlineMatches(deadlineName)
            ));
        }
    }

    private Consumer<ScheduledExecution<DbSchedulerBinaryDeadlineDetails>> cancelIfBinaryDeadlineMatches(
            String deadlineName
    ) {
        return scheduledExecution -> {
            if (deadlineName.equals(scheduledExecution.getData().getD())) {
                scheduler.cancel(scheduledExecution.getTaskInstance());
            }
        };
    }

    private Consumer<ScheduledExecution<DbSchedulerHumanReadableDeadlineDetails>> cancelIfHumanReadableDeadlineMatches(
            String deadlineName
    ) {
        return scheduledExecution -> {
            if (deadlineName.equals(scheduledExecution.getData().getDeadlineName())) {
                scheduler.cancel(scheduledExecution.getTaskInstance());
            }
        };
    }

    /**
     * {@inheritDoc}
     * <p>
     * The given {@code scope} is converted to its stored form, and compared with the stored form of each scheduled
     * deadline's scope. The converter therefore has to produce exactly the form the stored scopes were written in. The
     * stored class name of the scope is compared as well, as descriptors of different classes, such as an
     * {@link org.axonframework.modelling.command.AggregateScopeDescriptor} and a
     * {@link org.axonframework.modelling.saga.SagaScopeDescriptor} of equal type and identifier, can have the same
     * stored form. Axon Framework 4 compared the stored form only, and cancelled both.
     */
    @Override
    public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
        if (useBinaryPojo) {
            runOnPrepareCommitOrNow(context -> scheduler.fetchScheduledExecutionsForTask(
                    TASK_NAME,
                    DbSchedulerBinaryDeadlineDetails.class,
                    cancelIfDeadlineAndScopeMatches(deadlineName,
                                                    scope.getClass().getName(),
                                                    Objects.requireNonNull(converter.toStored(scope, byte[].class)))
            ));
        } else {
            runOnPrepareCommitOrNow(context -> scheduler.fetchScheduledExecutionsForTask(
                    TASK_NAME,
                    DbSchedulerHumanReadableDeadlineDetails.class,
                    cancelIfDeadlineAndScopeMatches(deadlineName,
                                                    scope.getClass().getName(),
                                                    Objects.requireNonNull(converter.toStored(scope, String.class)))
            ));
        }
    }

    private Consumer<ScheduledExecution<DbSchedulerHumanReadableDeadlineDetails>> cancelIfDeadlineAndScopeMatches(
            String deadlineName,
            String scopeClassName,
            String scopeDescriptor
    ) {
        return scheduledExecution -> {
            DbSchedulerHumanReadableDeadlineDetails data = scheduledExecution.getData();
            if (deadlineName.equals(data.getDeadlineName())
                    && scopeClassName.equals(data.getScopeDescriptorClass())
                    && scopeDescriptor.equals(data.getScopeDescriptor())) {
                scheduler.cancel(scheduledExecution.getTaskInstance());
            }
        };
    }

    private Consumer<ScheduledExecution<DbSchedulerBinaryDeadlineDetails>> cancelIfDeadlineAndScopeMatches(
            String deadlineName,
            String scopeClassName,
            byte[] scopeDescriptor
    ) {
        return scheduledExecution -> {
            DbSchedulerBinaryDeadlineDetails data = scheduledExecution.getData();
            if (deadlineName.equals(data.getD())
                    && scopeClassName.equals(data.getSc())
                    && Arrays.equals(scopeDescriptor, data.getS())) {
                scheduler.cancel(scheduledExecution.getTaskInstance());
            }
        };
    }

    private void execute(DbSchedulerBinaryDeadlineDetails deadlineDetails) {
        GenericDeadlineMessage deadlineMessage = deadlineDetails.asDeadLineMessage(converter);
        ScopeDescriptor scopeDescriptor = deadlineDetails.getDeserializedScopeDescriptor(converter);
        execute(deadlineDetails.getD(), deadlineMessage, scopeDescriptor);
    }

    private void execute(DbSchedulerHumanReadableDeadlineDetails deadlineDetails) {
        GenericDeadlineMessage deadlineMessage = deadlineDetails.asDeadLineMessage(converter);
        ScopeDescriptor scopeDescriptor = deadlineDetails.getDeserializedScopeDescriptor(converter);
        execute(deadlineDetails.getDeadlineName(), deadlineMessage, scopeDescriptor);
    }

    private void execute(String deadlineName, GenericDeadlineMessage deadlineMessage, ScopeDescriptor scopeDescriptor) {
        try {
            delivery.deliver(deadlineMessage, scopeDescriptor);
        } catch (Exception e) {
            logger.warn("An error occurred while triggering deadline with name [{}].", deadlineName);
            throw new DeadlineException("Failed to process", e);
        }
    }

    /**
     * Will start the {@link Scheduler} depending on its current state and the value of {@code startScheduler}.
     */
    public void start() {
        if (!startScheduler) {
            return;
        }
        SchedulerState state = scheduler.getSchedulerState();
        if (state.isShuttingDown()) {
            logger.warn("Scheduler is shutting down - will not attempting to start");
            return;
        }
        if (state.isStarted()) {
            logger.info("Scheduler already started - will not attempt to start again");
            return;
        }
        logger.info("Triggering scheduler start");
        scheduler.start();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Stops the {@link Scheduler} once, unless {@code stopScheduler} is {@code false}.
     */
    @Override
    public void shutdown() {
        if (isShutdown.compareAndSet(false, true) && stopScheduler) {
            scheduler.stop();
        }
    }

    /**
     * Builder class to instantiate a {@link DbSchedulerDeadlineManager}.
     * <p>
     * The {@code useBinaryPojo}, {@code startScheduler} and {@code stopScheduler} settings are defaulted to
     * {@code true}. The {@link Scheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory} and {@link Converter}
     * are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private @Nullable Scheduler scheduler;
        private @Nullable ScopeAwareProvider scopeAwareProvider;
        private @Nullable UnitOfWorkFactory unitOfWorkFactory;
        private @Nullable Converter converter;
        private boolean useBinaryPojo = true;
        private boolean startScheduler = true;
        private boolean stopScheduler = true;

        /**
         * Sets the {@link Scheduler} used for scheduling and triggering purposes of deadlines. It should have either
         * the {@link #binaryTask(Supplier)} or the {@link #humanReadableTask(Supplier)} from this class as one of its
         * tasks to work. Which one depends on the setting of {@code useBinaryPojo}. When {@code true}, use
         * {@link #binaryTask(Supplier)} else {@link #humanReadableTask(Supplier)}.
         *
         * @param scheduler a {@link Scheduler} used for scheduling and triggering purposes of the deadlines
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder scheduler(Scheduler scheduler) {
            assertNonNull(scheduler, "Scheduler may not be null");
            this.scheduler = scheduler;
            return this;
        }

        /**
         * Sets the {@link ScopeAwareProvider} which is capable of providing a stream of {@link Scope} instances for a
         * given {@link ScopeDescriptor}. Used to return the right Scope to trigger a deadline in.
         *
         * @param scopeAwareProvider a {@link ScopeAwareProvider} used to find the right {@link Scope} to trigger a
         *                           deadline in
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder scopeAwareProvider(ScopeAwareProvider scopeAwareProvider) {
            assertNonNull(scopeAwareProvider, "ScopeAwareProvider may not be null");
            this.scopeAwareProvider = scopeAwareProvider;
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
         * Sets the {@link Converter} used to convert the payload, metadata and {@link ScopeDescriptor} of a deadline to
         * and from their stored form in the task data. To keep reading tasks scheduled by Axon Framework 4, it has to
         * match the serializer the Axon Framework 4 deadline manager used, such as a
         * {@link org.axonframework.conversion.jackson.JacksonConverter} for a {@code JacksonSerializer}. For a manager
         * that Axon Framework 4's Spring Boot auto-configuration built, that is the counterpart of the event
         * serializer, the {@link org.axonframework.messaging.eventhandling.conversion.EventConverter}.
         *
         * @param converter the {@link Converter} used to convert the deadline's parts
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder converter(Converter converter) {
            assertNonNull(converter, "Converter may not be null");
            this.converter = converter;
            return this;
        }

        /**
         * Sets whether to use a pojo optimized for size, {@link DbSchedulerBinaryDeadlineDetails}, compared to a pojo
         * optimized for readability, {@link DbSchedulerHumanReadableDeadlineDetails}. Defaults to {@code true}.
         *
         * @param useBinaryPojo a {@code boolean} to determine whether to use a binary format
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder useBinaryPojo(boolean useBinaryPojo) {
            this.useBinaryPojo = useBinaryPojo;
            return this;
        }

        /**
         * Sets whether {@link DbSchedulerDeadlineManager#start()} starts the {@link Scheduler}, or leaves starting it
         * to the application. Defaults to {@code true}.
         *
         * @param startScheduler a {@code boolean} to determine whether to start the scheduler
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder startScheduler(boolean startScheduler) {
            this.startScheduler = startScheduler;
            return this;
        }

        /**
         * Sets whether {@link DbSchedulerDeadlineManager#shutdown()} stops the {@link Scheduler}, or leaves stopping it
         * to the application. Defaults to {@code true}.
         *
         * @param stopScheduler a {@code boolean} to determine whether to stop the scheduler
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder stopScheduler(boolean stopScheduler) {
            this.stopScheduler = stopScheduler;
            return this;
        }

        /**
         * Initializes a {@link DbSchedulerDeadlineManager} as specified through this Builder.
         *
         * @return a {@link DbSchedulerDeadlineManager} as specified through this Builder
         */
        public DbSchedulerDeadlineManager build() {
            return new DbSchedulerDeadlineManager(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(scopeAwareProvider, "The ScopeAwareProvider is a hard requirement and should be provided.");
            assertNonNull(scheduler, "The Scheduler is a hard requirement and should be provided.");
            assertNonNull(unitOfWorkFactory, "The UnitOfWorkFactory is a hard requirement and should be provided.");
            assertNonNull(converter, "The Converter is a hard requirement and should be provided.");
        }
    }
}
