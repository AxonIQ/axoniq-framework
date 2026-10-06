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

package org.axonframework.deadline.quartz;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.AxonNonTransientException;
import org.axonframework.conversion.Converter;
import org.axonframework.deadline.AbstractDeadlineManager;
import org.axonframework.deadline.DeadlineDelivery;
import org.axonframework.deadline.DeadlineException;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.messaging.Scope;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Date.from;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.ExceptionUtils.findException;
import static org.quartz.JobKey.jobKey;

/**
 * Implementation of {@link DeadlineManager} that delegates scheduling and triggering to a Quartz {@link Scheduler}.
 * <p>
 * Each deadline is stored as a Quartz job of type {@link DeadlineJob}, in the job group named after the deadline. The
 * job data keeps the layout of Axon Framework 4.13, so that jobs scheduled by Axon Framework 4 fire here, and jobs
 * scheduled here fire on Axon Framework 4 nodes sharing the scheduler's store. The payload, metadata and scope
 * descriptor are converted with the configured {@link Converter}, which has to match the serializer the Axon Framework
 * 4 deadline manager used.
 * <p>
 * A fired deadline runs in a unit of work from the configured {@link UnitOfWorkFactory}, with the registered handler
 * interceptors around its delivery to the {@link org.axonframework.messaging.ScopeAware} components of the
 * {@link ScopeAwareProvider}. A failing delivery is refired immediately, unless the
 * {@link Builder#refireImmediatelyPolicy(Predicate) refire policy} decides otherwise.
 * <pre>{@code
 * QuartzDeadlineManager deadlineManager =
 *         QuartzDeadlineManager.builder()
 *                              .scheduler(scheduler)
 *                              .scopeAwareProvider(scopeAwareProvider)
 *                              .unitOfWorkFactory(configuration.getComponent(UnitOfWorkFactory.class))
 *                              .converter(new JacksonConverter())
 *                              .build();
 * }</pre>
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @author Jakob Hatzl
 * @since 3.3
 */
public class QuartzDeadlineManager extends AbstractDeadlineManager {

    private static final Logger logger = LoggerFactory.getLogger(QuartzDeadlineManager.class);
    private static final String CANCEL_ERROR_MESSAGE =
            "An error occurred while cancelling a timer for a deadline manager";

    private static final String JOB_NAME_PREFIX = "deadline-";

    private final Scheduler scheduler;
    private final Converter converter;
    private final StoredDeadlineConverter storedDeadlineConverter;
    private final DeadlineDelivery delivery;
    private final Predicate<Throwable> refireImmediatelyPolicy;

    /**
     * Instantiate a Builder to be able to create a {@code QuartzDeadlineManager}.
     * <p>
     * The refire policy defaults to refiring immediately for every failure that is not an
     * {@link AxonNonTransientException}. The {@link Scheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory}
     * and {@link Converter} are <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@code QuartzDeadlineManager}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Instantiate a {@code QuartzDeadlineManager} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link Scheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory} and
     * {@link Converter} are not {@code null}, and will throw an {@link AxonConfigurationException} if any of them is
     * {@code null}. The components the {@link DeadlineJob} needs are tied to the Scheduler's context. If this
     * initialization step fails, this will too result in an AxonConfigurationException.
     *
     * @param builder the {@link Builder} used to instantiate a {@code QuartzDeadlineManager} instance
     */
    protected QuartzDeadlineManager(Builder builder) {
        builder.validate();
        this.scheduler = Objects.requireNonNull(builder.scheduler);
        this.converter = Objects.requireNonNull(builder.converter);
        this.storedDeadlineConverter = new StoredDeadlineConverter(converter);
        this.delivery = new DeadlineDelivery(Objects.requireNonNull(builder.unitOfWorkFactory),
                                             Objects.requireNonNull(builder.scopeAwareProvider),
                                             handlerInterceptors());
        this.refireImmediatelyPolicy = builder.refireImmediatelyPolicy;

        try {
            initialize();
        } catch (SchedulerException e) {
            throw new AxonConfigurationException("Unable to initialize QuartzDeadlineManager", e);
        }
    }

    private void initialize() throws SchedulerException {
        scheduler.getContext().put(DeadlineJob.JOB_DATA_CONVERTER, storedDeadlineConverter);
        scheduler.getContext().put(DeadlineJob.DEADLINE_DELIVERY, delivery);
        scheduler.getContext().put(DeadlineJob.REFIRE_IMMEDIATELY_POLICY, refireImmediatelyPolicy);
    }

    @Override
    public String schedule(Instant triggerDateTime,
                           String deadlineName,
                           @Nullable Object messageOrPayload,
                           ScopeDescriptor deadlineScope) {
        DeadlineMessage deadlineMessage = asDeadlineMessage(deadlineName, messageOrPayload, triggerDateTime);
        String deadlineId = JOB_NAME_PREFIX + deadlineMessage.identifier();

        runOnPrepareCommitOrNow(context -> {
            DeadlineMessage interceptedDeadlineMessage = processDispatchInterceptors(deadlineMessage, context);
            try {
                JobDetail jobDetail = buildJobDetail(interceptedDeadlineMessage,
                                                     deadlineScope,
                                                     new JobKey(deadlineId, deadlineName));
                scheduler.scheduleJob(jobDetail, buildTrigger(triggerDateTime, jobDetail.getKey()));
            } catch (SchedulerException e) {
                throw new DeadlineException("An error occurred while setting a timer for a deadline", e);
            }
        });

        return deadlineId;
    }

    @Override
    public void cancelSchedule(String deadlineName, String scheduleId) {
        runOnPrepareCommitOrNow(context -> cancelSchedule(jobKey(scheduleId, deadlineName)));
    }

    @Override
    public void cancelAll(String deadlineName) {
        runOnPrepareCommitOrNow(context -> {
            try {
                scheduler.getJobKeys(GroupMatcher.groupEquals(deadlineName))
                         .forEach(this::cancelSchedule);
            } catch (SchedulerException e) {
                throw new DeadlineException(CANCEL_ERROR_MESSAGE, e);
            }
        });
    }

    /**
     * {@inheritDoc}
     * <p>
     * Cancels immediately, as Axon Framework 4 did, even when called while a Saga is handled. Each stored scope is
     * converted back into its class and compared with the given {@code scope} through {@code equals}. The given scope
     * is first converted to its stored form and back, so that both sides lose the same type information, such as the
     * {@link java.util.UUID} type of an identifier that JSON stores as a string.
     */
    @Override
    public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
        ScopeDescriptor givenScope = Objects.requireNonNull(
                converter.convert(converter.convert(scope, String.class), scope.getClass())
        );
        try {
            Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(deadlineName));
            for (JobKey jobKey : jobKeys) {
                JobDetail jobDetail = scheduler.getJobDetail(jobKey);
                ScopeDescriptor jobScope = DeadlineJob.DeadlineJobDataBinder
                        .deadlineScope(storedDeadlineConverter, jobDetail.getJobDataMap());
                if (givenScope.equals(jobScope)) {
                    cancelSchedule(jobKey);
                }
            }
        } catch (SchedulerException e) {
            throw new DeadlineException(CANCEL_ERROR_MESSAGE, e);
        }
    }

    private void cancelSchedule(JobKey jobKey) {
        try {
            if (!scheduler.deleteJob(jobKey)) {
                logger.warn("The job belonging to this token could not be deleted.");
            }
        } catch (SchedulerException e) {
            throw new DeadlineException(CANCEL_ERROR_MESSAGE, e);
        }
    }

    private JobDetail buildJobDetail(DeadlineMessage deadlineMessage, ScopeDescriptor deadlineScope, JobKey jobKey) {
        JobDataMap jobData = DeadlineJob.DeadlineJobDataBinder.toJobData(storedDeadlineConverter,
                                                                         deadlineMessage,
                                                                         deadlineScope);
        return JobBuilder.newJob(DeadlineJob.class)
                         .withDescription(deadlineMessage.payloadType().getName())
                         .withIdentity(jobKey)
                         .usingJobData(jobData)
                         .requestRecovery(true)
                         .build();
    }

    private static Trigger buildTrigger(Instant triggerDateTime, JobKey key) {
        return TriggerBuilder.newTrigger()
                             .forJob(key)
                             .startAt(from(triggerDateTime))
                             .build();
    }

    @Override
    public void shutdown() {
        try {
            scheduler.shutdown(true);
        } catch (SchedulerException e) {
            throw new DeadlineException("An error occurred while trying to shutdown the deadline manager", e);
        }
    }

    /**
     * Builder class to instantiate a {@link QuartzDeadlineManager}.
     * <p>
     * The refire policy defaults to refiring immediately for every failure that is not an
     * {@link AxonNonTransientException}. The {@link Scheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory}
     * and {@link Converter} are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private @Nullable Scheduler scheduler;
        private @Nullable ScopeAwareProvider scopeAwareProvider;
        private @Nullable UnitOfWorkFactory unitOfWorkFactory;
        private @Nullable Converter converter;
        private Predicate<Throwable> refireImmediatelyPolicy =
                throwable -> findException(throwable, AxonNonTransientException.class::isInstance).isEmpty();

        /**
         * Sets the {@link Scheduler} used for scheduling and triggering purposes of the deadlines.
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
         * and from the {@link JobDataMap}. To keep reading jobs scheduled by Axon Framework 4, it has to match the
         * serializer the Axon Framework 4 deadline manager used, such as a
         * {@link org.axonframework.conversion.jackson.JacksonConverter} for a {@code JacksonSerializer}.
         *
         * @param converter the {@link Converter} used to convert the deadline's data to and from the job data
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder converter(Converter converter) {
            assertNonNull(converter, "Converter may not be null");
            this.converter = converter;
            return this;
        }

        /**
         * Sets a {@link Predicate} taking a {@link Throwable} to decide whether a failed {@link DeadlineJob} should be
         * 'refired' immediately. Defaults to a Predicate which will refire immediately on
         * non-{@link AxonNonTransientException}s.
         *
         * @param refireImmediatelyPolicy a {@link Predicate} taking a {@link Throwable} to decide whether a failed
         *                                {@link DeadlineJob} should be 'refired' immediately
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder refireImmediatelyPolicy(Predicate<Throwable> refireImmediatelyPolicy) {
            assertNonNull(refireImmediatelyPolicy, "The refire policy may not be null");
            this.refireImmediatelyPolicy = refireImmediatelyPolicy;
            return this;
        }

        /**
         * Initializes a {@link QuartzDeadlineManager} as specified through this Builder.
         *
         * @return a {@link QuartzDeadlineManager} as specified through this Builder
         */
        public QuartzDeadlineManager build() {
            return new QuartzDeadlineManager(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(scheduler, "The Scheduler is a hard requirement and should be provided");
            assertNonNull(scopeAwareProvider, "The ScopeAwareProvider is a hard requirement and should be provided");
            assertNonNull(unitOfWorkFactory, "The UnitOfWorkFactory is a hard requirement and should be provided");
            assertNonNull(converter, "The Converter is a hard requirement and should be provided");
        }
    }
}
