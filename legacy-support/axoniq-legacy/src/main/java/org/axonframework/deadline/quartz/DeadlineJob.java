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

import org.axonframework.deadline.DeadlineDelivery;
import org.axonframework.deadline.DeadlineException;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.messaging.Scope;
import org.axonframework.messaging.ScopeDescriptor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.function.Predicate;

import static org.axonframework.deadline.quartz.DeadlineJob.DeadlineJobDataBinder.deadlineMessage;
import static org.axonframework.deadline.quartz.DeadlineJob.DeadlineJobDataBinder.deadlineScope;

/**
 * Quartz job which depicts handling of a scheduled deadline message. The {@link DeadlineMessage} and
 * {@link ScopeDescriptor} are retrieved from the {@link JobExecutionContext}. The components to convert and deliver
 * them are fetched from the {@link SchedulerContext}, where the {@link QuartzDeadlineManager} put them.
 * <p>
 * The job data is read before the deadline's unit of work starts. Job data that cannot be read fails the job, which
 * Quartz then does not refire, as in Axon Framework 4.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @since 3.3
 */
public class DeadlineJob implements Job {

    private static final Logger logger = LoggerFactory.getLogger(DeadlineJob.class);

    /**
     * The key under which the {@link StoredDeadlineConverter} is stored within the {@link SchedulerContext}.
     */
    public static final String JOB_DATA_CONVERTER = StoredDeadlineConverter.class.getName();

    /**
     * The key under which the {@link DeadlineDelivery} is stored within the {@link SchedulerContext}.
     */
    public static final String DEADLINE_DELIVERY = DeadlineDelivery.class.getName();

    /**
     * The key under which a {@link Predicate} is stored within the {@link SchedulerContext}. Used to decide whether a
     * job should be 'refired' immediately for a given {@link Throwable}.
     */
    public static final String REFIRE_IMMEDIATELY_POLICY = "refireImmediatelyPolicy";

    private static final Boolean REFIRE_IMMEDIATELY = true;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting a deadline job");
        }

        JobDetail jobDetail = context.getJobDetail();
        JobDataMap jobData = jobDetail.getJobDataMap();

        SchedulerContext schedulerContext;
        try {
            schedulerContext = context.getScheduler().getContext();
        } catch (Exception e) {
            logger.error("Exception occurred during processing a deadline job [{}]", jobDetail.getDescription(), e);
            throw new JobExecutionException(e);
        }

        StoredDeadlineConverter converter = (StoredDeadlineConverter) schedulerContext.get(JOB_DATA_CONVERTER);
        DeadlineDelivery delivery = (DeadlineDelivery) schedulerContext.get(DEADLINE_DELIVERY);

        DeadlineMessage deadlineMessage = deadlineMessage(converter, jobData);
        ScopeDescriptor deadlineScope = deadlineScope(converter, jobData);

        try {
            delivery.deliver(deadlineMessage, deadlineScope);
        } catch (Exception exceptionResult) {
            @SuppressWarnings("unchecked")
            Predicate<Throwable> refirePolicy = (Predicate<Throwable>) schedulerContext.get(REFIRE_IMMEDIATELY_POLICY);
            if (refirePolicy.test(exceptionResult)) {
                logger.error("Exception occurred during processing a deadline job which will be retried [{}]",
                             jobDetail.getDescription(), exceptionResult);
                throw new JobExecutionException(exceptionResult, REFIRE_IMMEDIATELY);
            }
            logger.error("Exception occurred during processing a deadline job [{}]",
                         jobDetail.getDescription(), exceptionResult);
            throw new JobExecutionException(exceptionResult);
        }
        if (logger.isInfoEnabled()) {
            logger.info("Job successfully executed. Deadline message [{}] processed.",
                        deadlineMessage.payloadType().getSimpleName());
        }
    }

    /**
     * This binder is used to map deadline message and deadline scopes to the job data and vice versa.
     * <p>
     * The keys are those of Axon Framework 4.13, so that both versions read each other's jobs.
     */
    public static class DeadlineJobDataBinder {

        /**
         * Key pointing to the serialized deadline {@link ScopeDescriptor} in the {@link JobDataMap}
         */
        public static final String SERIALIZED_DEADLINE_SCOPE = "serializedDeadlineScope";
        /**
         * Key pointing to the class name of the deadline {@link ScopeDescriptor} in the {@link JobDataMap}
         */
        public static final String SERIALIZED_DEADLINE_SCOPE_CLASS_NAME = "serializedDeadlineScopeClassName";
        /**
         * Key pointing to a message identifier.
         */
        public static final String MESSAGE_ID = "axon-message-id";
        /**
         * Key pointing to the serialized payload of a message.
         */
        public static final String SERIALIZED_MESSAGE_PAYLOAD = "axon-serialized-message-payload";
        /**
         * Key pointing to the payload type of a message.
         */
        public static final String MESSAGE_TYPE = "axon-message-type";
        /**
         * Key pointing to the revision of a message.
         */
        public static final String MESSAGE_REVISION = "axon-message-revision";
        /**
         * Key pointing to the timestamp of a message.
         */
        public static final String MESSAGE_TIMESTAMP = "axon-message-timestamp";
        /**
         * Key pointing to the metadata of a message.
         */
        public static final String MESSAGE_METADATA = "axon-metadata";
        /**
         * Key pointing to the deadline name of a {@link DeadlineMessage}.
         */
        public static final String DEADLINE_NAME = "axon-deadline-name";

        /**
         * The key under which Axon Framework 3.3 stored the whole serialized deadline message. Such jobs are not read.
         */
        private static final String AXON_3_3_SERIALIZED_DEADLINE_MESSAGE = "serializedDeadlineMessage";

        /**
         * Converts the provided {@code deadlineMessage} and {@code deadlineScope} and puts them in a
         * {@link JobDataMap}.
         *
         * @param converter       the converter used to convert the given {@code deadlineMessage} and
         *                        {@code deadlineScope} to their stored form
         * @param deadlineMessage the {@link DeadlineMessage} to be handled
         * @param deadlineScope   the {@link ScopeDescriptor} of the {@link Scope} the {@code deadlineMessage} should go
         *                        to
         * @return a {@link JobDataMap} containing the {@code deadlineMessage} and {@code deadlineScope}
         */
        public static JobDataMap toJobData(StoredDeadlineConverter converter,
                                           DeadlineMessage deadlineMessage,
                                           ScopeDescriptor deadlineScope) {
            JobDataMap jobData = new JobDataMap();
            putDeadlineMessage(jobData, deadlineMessage, converter);
            putDeadlineScope(jobData, deadlineScope, converter);
            return jobData;
        }

        private static void putDeadlineMessage(JobDataMap jobData,
                                               DeadlineMessage deadlineMessage,
                                               StoredDeadlineConverter converter) {
            jobData.put(DEADLINE_NAME, deadlineMessage.getDeadlineName());
            jobData.put(MESSAGE_ID, deadlineMessage.identifier());
            jobData.put(MESSAGE_TIMESTAMP, deadlineMessage.timestamp().toString());

            Object payload = deadlineMessage.payload();
            jobData.put(SERIALIZED_MESSAGE_PAYLOAD, (Object) converter.toStored(payload, byte[].class));
            jobData.put(MESSAGE_TYPE, StoredDeadlineConverter.typeNameOf(payload));
            jobData.put(MESSAGE_REVISION, (String) null);

            jobData.put(MESSAGE_METADATA, (Object) converter.toStored(deadlineMessage.metadata(), byte[].class));
        }

        private static void putDeadlineScope(JobDataMap jobData,
                                             ScopeDescriptor deadlineScope,
                                             StoredDeadlineConverter converter) {
            jobData.put(SERIALIZED_DEADLINE_SCOPE, (Object) converter.toStored(deadlineScope, byte[].class));
            jobData.put(SERIALIZED_DEADLINE_SCOPE_CLASS_NAME, deadlineScope.getClass().getName());
        }

        /**
         * Extracts a {@link DeadlineMessage} from provided {@code jobDataMap}.
         *
         * @param converter  the converter used to convert the contents of the given {@code jobDataMap} into a
         *                   {@link DeadlineMessage}
         * @param jobDataMap the {@link JobDataMap} which should contain a {@link DeadlineMessage}
         * @return the {@link DeadlineMessage} pulled from the {@code jobDataMap}
         * @throws DeadlineException if the job was stored in the format of Axon Framework 3.3, which is not read
         */
        public static DeadlineMessage deadlineMessage(StoredDeadlineConverter converter, JobDataMap jobDataMap) {
            if (jobDataMap.containsKey(AXON_3_3_SERIALIZED_DEADLINE_MESSAGE)) {
                throw new DeadlineException(
                        "The deadline job was stored in the format of Axon Framework 3.3, which is not supported. "
                                + "Reschedule the deadline."
                );
            }
            Object payload = converter.payload((String) jobDataMap.get(MESSAGE_TYPE),
                                               (String) jobDataMap.get(MESSAGE_REVISION),
                                               jobDataMap.get(SERIALIZED_MESSAGE_PAYLOAD));
            return new GenericDeadlineMessage((String) jobDataMap.get(DEADLINE_NAME),
                                              (String) jobDataMap.get(MESSAGE_ID),
                                              StoredDeadlineConverter.messageTypeOf(payload),
                                              payload,
                                              converter.metadata(jobDataMap.get(MESSAGE_METADATA)),
                                              retrieveDeadlineTimestamp(jobDataMap));
        }

        private static Instant retrieveDeadlineTimestamp(JobDataMap jobDataMap) {
            Object timestamp = jobDataMap.get(MESSAGE_TIMESTAMP);
            if (timestamp instanceof String) {
                return Instant.parse(timestamp.toString());
            }
            return Instant.ofEpochMilli((long) timestamp);
        }

        /**
         * Extracts a {@link ScopeDescriptor} describing the deadline {@link Scope}, pulled from provided
         * {@code jobDataMap}.
         *
         * @param converter  the converter used to convert the contents of the given {@code jobDataMap} into a
         *                   {@link ScopeDescriptor}
         * @param jobDataMap the {@link JobDataMap} which should contain a {@link ScopeDescriptor}
         * @return the {@link ScopeDescriptor} describing the deadline {@link Scope}, pulled from provided
         * {@code jobDataMap}
         */
        public static ScopeDescriptor deadlineScope(StoredDeadlineConverter converter, JobDataMap jobDataMap) {
            return converter.scope((String) jobDataMap.get(SERIALIZED_DEADLINE_SCOPE_CLASS_NAME),
                                   jobDataMap.get(SERIALIZED_DEADLINE_SCOPE));
        }
    }
}
