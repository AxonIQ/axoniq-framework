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

package org.axonframework.deadline.jobrunr;

import org.axonframework.common.AxonConfigurationException;
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
import org.jobrunr.jobs.JobId;
import org.jobrunr.jobs.states.IllegalJobStateChangeException;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.JobBuilder;
import org.jobrunr.scheduling.JobScheduler;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.deadline.jobrunr.LabelUtils.getCombinedLabel;
import static org.axonframework.deadline.jobrunr.LabelUtils.getLabel;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of {@link DeadlineManager} that delegates scheduling and triggering to a JobRunr
 * {@link JobScheduler}.
 * <p>
 * Each deadline is stored as a JobRunr job calling {@link #execute(String, String)} on this manager, with the
 * deadline's {@link DeadlineDetails} in their stored form as the first argument. The details keep the layout of Axon
 * Framework 4.13, so that jobs scheduled by Axon Framework 4 fire here, and jobs scheduled here fire on Axon Framework
 * 4 nodes sharing JobRunr's storage. The details and the deadline's payload, metadata and scope descriptor are
 * converted with the configured {@link Converter}, which has to match the serializer the Axon Framework 4 deadline
 * manager used.
 * <p>
 * A fired deadline runs in a unit of work from the configured {@link UnitOfWorkFactory}, with the registered handler
 * interceptors around its delivery to the {@link org.axonframework.messaging.ScopeAware} components of the
 * {@link ScopeAwareProvider}. A failing delivery fails the job, which JobRunr then retries.
 * <p>
 * {@link #cancelAll(String)} and {@link #cancelAllWithinScope(String, ScopeDescriptor)} are not supported, as they need
 * JobRunr Pro. Keep the identifier {@link #schedule(Instant, String, Object, ScopeDescriptor)} returns to cancel a
 * deadline with {@link #cancelSchedule(String, String)} instead.
 * <pre>{@code
 * JobRunrDeadlineManager deadlineManager =
 *         JobRunrDeadlineManager.builder()
 *                               .jobScheduler(jobScheduler)
 *                               .scopeAwareProvider(scopeAwareProvider)
 *                               .unitOfWorkFactory(configuration.getComponent(UnitOfWorkFactory.class))
 *                               .converter(new JacksonConverter())
 *                               .build();
 * }</pre>
 *
 * @author Tom de Backer
 * @author Gerard Klijs
 * @author Jakob Hatzl
 * @since 4.7.0
 */
public class JobRunrDeadlineManager extends AbstractDeadlineManager {

    private static final Logger logger = getLogger(JobRunrDeadlineManager.class);
    /**
     * The reason JobRunr records for a job deleted through {@link #cancelSchedule(String, String)}.
     */
    protected static final String DELETE_REASON = "Deleted via Axon DeadlineManager API";
    private static final String NOT_SUPPORTED_MSG =
            "The '%s' method is not supported without using JobRunrPro with the JobRunrProDeadlineManager.\n"
                    + "Move to the pro version and the extension or use 'cancelSchedule' method instead.\n"
                    + "Using 'cancelSchedule' requires keeping track of the returned 'scheduleId' "
                    + "from invoking 'schedule'.";

    private final JobScheduler jobScheduler;
    private final StoredDeadlineConverter converter;
    private final DeadlineDelivery delivery;

    /**
     * Instantiate a Builder to be able to create a {@code JobRunrDeadlineManager}.
     * <p>
     * The {@link JobScheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory} and {@link Converter} are
     * <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@code JobRunrDeadlineManager}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Instantiate a {@code JobRunrDeadlineManager} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link JobScheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory} and
     * {@link Converter} are not {@code null}, and will throw an {@link AxonConfigurationException} if any of them is
     * {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@code JobRunrDeadlineManager} instance
     */
    protected JobRunrDeadlineManager(Builder builder) {
        builder.validate();
        this.jobScheduler = Objects.requireNonNull(builder.jobScheduler);
        this.converter = new StoredDeadlineConverter(Objects.requireNonNull(builder.converter));
        this.delivery = new DeadlineDelivery(Objects.requireNonNull(builder.unitOfWorkFactory),
                                             Objects.requireNonNull(builder.scopeAwareProvider),
                                             handlerInterceptors());
    }

    @Override
    public String schedule(Instant triggerDateTime,
                           String deadlineName,
                           @Nullable Object messageOrPayload,
                           ScopeDescriptor deadlineScope) {
        DeadlineMessage deadlineMessage = asDeadlineMessage(deadlineName, messageOrPayload, triggerDateTime);
        UUID deadlineId = UUID.randomUUID();
        runOnPrepareCommitOrNow(context -> {
            DeadlineMessage interceptedDeadlineMessage = processDispatchInterceptors(deadlineMessage, context);
            String serializedDeadlineDetails = DeadlineDetails.serialized(deadlineName,
                                                                          deadlineScope,
                                                                          interceptedDeadlineMessage,
                                                                          converter);
            String combinedLabel = getCombinedLabel(converter, deadlineName, deadlineScope);
            JobBuilder job = JobBuilder.aJob()
                                       .withId(deadlineId)
                                       .withName(deadlineName)
                                       .withLabels(getLabel(deadlineName), combinedLabel)
                                       .withDetails(() -> this.execute(serializedDeadlineDetails,
                                                                       deadlineId.toString()))
                                       .scheduleAt(triggerDateTime);
            JobId id = jobScheduler.create(job);
            logger.debug("Job with id: [{}] was successfully created.", id);
        });
        return deadlineId.toString();
    }

    @Override
    public void cancelSchedule(String deadlineName, String scheduleId) {
        runOnPrepareCommitOrNow(context -> {
            try {
                jobScheduler.delete(toUuid(scheduleId), DELETE_REASON);
            } catch (IllegalJobStateChangeException e) {
                if (!tryingToDeleteAlreadyDeletedJob(e.getFrom(), e.getTo())) {
                    throw e;
                }
            }
        });
    }

    /**
     * When a job is tried to be deleted, which is already deleted, an exception is thrown. As the result is the same,
     * this exception can be ignored.
     */
    private static boolean tryingToDeleteAlreadyDeletedJob(StateName from, StateName to) {
        return from == StateName.DELETED && to == StateName.DELETED;
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException always, as cancelling by deadline name needs JobRunr Pro
     */
    @Override
    public void cancelAll(String deadlineName) {
        throw new UnsupportedOperationException(String.format(NOT_SUPPORTED_MSG, "cancelAll"));
    }

    private UUID toUuid(String scheduleId) {
        try {
            return UUID.fromString(scheduleId);
        } catch (IllegalArgumentException e) {
            throw new DeadlineException("For JobRunr the scheduleId should be an UUID representation.", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws UnsupportedOperationException always, as cancelling by {@link Scope} needs JobRunr Pro
     */
    @Override
    public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
        throw new UnsupportedOperationException(String.format(NOT_SUPPORTED_MSG, "cancelAllWithinScope"));
    }

    /**
     * This method is called by JobRunr for jobs scheduled by Axon Framework 4 before it passed the deadline
     * identifier. It fires the deadline as {@link #execute(String, String)} does.
     *
     * @param serializedDeadlineDetails the stored {@link DeadlineDetails} of the deadline to fire
     * @deprecated Kept so that jobs scheduled through this signature still fire. New jobs call
     * {@link #execute(String, String)}.
     */
    @Deprecated(since = "4.8.0")
    public void execute(String serializedDeadlineDetails) {
        execute(serializedDeadlineDetails, null);
    }

    /**
     * This method is called by JobRunr when a deadline fires. It converts the stored {@link DeadlineDetails} back into
     * the deadline, and delivers it to the components resolving its scope.
     * <p>
     * The signature stays exactly as in Axon Framework 4, as JobRunr stores the call to this method with every job.
     *
     * @param serializedDeadlineDetails the stored {@link DeadlineDetails} of the deadline to fire
     * @param deadlineId                the identifier of the deadline, which JobRunr uses as the job's identifier
     * @throws DeadlineException if delivering the deadline failed, so that JobRunr retries the job
     */
    public void execute(String serializedDeadlineDetails, @Nullable String deadlineId) {
        DeadlineDetails deadlineDetails = converter.fromStored(serializedDeadlineDetails, DeadlineDetails.class);
        GenericDeadlineMessage deadlineMessage = deadlineDetails.asDeadLineMessage(converter);
        ScopeDescriptor deadlineScope = deadlineDetails.getDeserializedScopeDescriptor(converter);
        try {
            delivery.deliver(deadlineMessage, deadlineScope);
        } catch (Exception e) {
            logger.warn("An error occurred while triggering deadline with name [{}]. Original message: [{}]",
                        deadlineDetails.getDeadlineName(), e.getMessage());
            throw new DeadlineException("Failed to process", e);
        }
    }

    @Override
    public void shutdown() {
        jobScheduler.shutdown();
    }

    /**
     * Builder class to instantiate a {@link JobRunrDeadlineManager}.
     * <p>
     * The {@link JobScheduler}, {@link ScopeAwareProvider}, {@link UnitOfWorkFactory} and {@link Converter} are
     * <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder {

        private @Nullable JobScheduler jobScheduler;
        private @Nullable ScopeAwareProvider scopeAwareProvider;
        private @Nullable UnitOfWorkFactory unitOfWorkFactory;
        private @Nullable Converter converter;

        /**
         * Sets the {@link JobScheduler} used for scheduling and triggering purposes of the deadlines.
         *
         * @param jobScheduler a {@link JobScheduler} used for scheduling and triggering purposes of the deadlines
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder jobScheduler(JobScheduler jobScheduler) {
            assertNonNull(jobScheduler, "JobScheduler may not be null");
            this.jobScheduler = jobScheduler;
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
         * Sets the {@link Converter} used to convert the {@link DeadlineDetails} of a deadline, and the payload,
         * metadata and {@link ScopeDescriptor} they hold. To keep reading jobs scheduled by Axon Framework 4, it has to
         * match the serializer the Axon Framework 4 deadline manager used, such as a
         * {@link org.axonframework.conversion.jackson.JacksonConverter} for a {@code JacksonSerializer}. For a manager
         * that Axon Framework 4's Spring Boot auto-configuration built, that is the counterpart of the event
         * serializer, the {@link org.axonframework.messaging.eventhandling.conversion.EventConverter}.
         *
         * @param converter the {@link Converter} used to convert the deadline's details
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder converter(Converter converter) {
            assertNonNull(converter, "Converter may not be null");
            this.converter = converter;
            return this;
        }

        /**
         * Initializes a {@link JobRunrDeadlineManager} as specified through this Builder.
         *
         * @return a {@link JobRunrDeadlineManager} as specified through this Builder
         */
        public JobRunrDeadlineManager build() {
            return new JobRunrDeadlineManager(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(scopeAwareProvider, "The ScopeAwareProvider is a hard requirement and should be provided.");
            assertNonNull(jobScheduler, "The JobScheduler is a hard requirement and should be provided.");
            assertNonNull(unitOfWorkFactory, "The UnitOfWorkFactory is a hard requirement and should be provided.");
            assertNonNull(converter, "The Converter is a hard requirement and should be provided.");
        }
    }
}
