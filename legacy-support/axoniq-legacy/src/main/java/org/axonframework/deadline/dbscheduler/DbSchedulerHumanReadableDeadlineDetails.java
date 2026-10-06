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

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.messaging.ScopeDescriptor;
import org.jspecify.annotations.Nullable;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Pojo that contains the needed information for a {@link com.github.kagkarlsson.scheduler.task.Task} handling a
 * deadline, in human-readable form. Will be stored by db-scheduler's own serializer, Java serialization by default, as
 * the data of the task created by {@link DbSchedulerDeadlineManager#humanReadableTask(java.util.function.Supplier)}.
 * The scope descriptor, payload and metadata it holds are converted with the configured
 * {@link org.axonframework.conversion.Converter}.
 * <p>
 * The field names and the {@code serialVersionUID} are those of Axon Framework 4.13, so that both versions read
 * each other's tasks.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
@SuppressWarnings("Duplicates")
public class DbSchedulerHumanReadableDeadlineDetails implements Serializable {

    /**
     * The value Java serialization computed for the Axon Framework 4.13 version of this class, which declared none.
     */
    @Serial
    private static final long serialVersionUID = 1231422756190086139L;

    private String deadlineName;
    private String scopeDescriptor;
    private String scopeDescriptorClass;
    private @Nullable String payload;
    private @Nullable String payloadClass;
    private @Nullable String payloadRevision;
    private @Nullable String metaData;

    @SuppressWarnings("NotNullFieldNotInitialized")
    DbSchedulerHumanReadableDeadlineDetails() {
        //no-args constructor needed for deserialization
    }

    /**
     * Creates a new {@code DbSchedulerHumanReadableDeadlineDetails} object from the stored form of a deadline's parts.
     *
     * @param deadlineName         the name of the deadline
     * @param scopeDescriptor      the stored {@link ScopeDescriptor} describing the scope of the deadline
     * @param scopeDescriptorClass the class name of the {@link ScopeDescriptor}
     * @param payload              the stored payload of the deadline, if any
     * @param payloadClass         the type name of the payload
     * @param payloadRevision      the revision of the payload type, if any
     * @param metaData             the stored metadata of the deadline
     */
    @SuppressWarnings("squid:S107")
    public DbSchedulerHumanReadableDeadlineDetails(String deadlineName,
                                                   String scopeDescriptor,
                                                   String scopeDescriptorClass,
                                                   @Nullable String payload,
                                                   @Nullable String payloadClass,
                                                   @Nullable String payloadRevision,
                                                   @Nullable String metaData) {
        this.deadlineName = deadlineName;
        this.scopeDescriptor = scopeDescriptor;
        this.scopeDescriptorClass = scopeDescriptorClass;
        this.payload = payload;
        this.payloadClass = payloadClass;
        this.payloadRevision = payloadRevision;
        this.metaData = metaData;
    }

    /**
     * Converts the given deadline into {@code DbSchedulerHumanReadableDeadlineDetails}.
     *
     * @param deadlineName the name of the deadline
     * @param descriptor   the {@link ScopeDescriptor} describing the scope of the deadline
     * @param message      the {@link DeadlineMessage} to store
     * @param converter    the converter used to convert the deadline's parts
     * @return the {@code DbSchedulerHumanReadableDeadlineDetails} of the given deadline
     */
    static DbSchedulerHumanReadableDeadlineDetails serialized(String deadlineName,
                                                       ScopeDescriptor descriptor,
                                                       DeadlineMessage message,
                                                       StoredDeadlineConverter converter) {
        Object payload = message.payload();
        return new DbSchedulerHumanReadableDeadlineDetails(
                deadlineName,
                Objects.requireNonNull(converter.toStored(descriptor, String.class)),
                descriptor.getClass().getName(),
                converter.toStored(payload, String.class),
                StoredDeadlineConverter.typeNameOf(payload),
                null,
                converter.toStored(message.metadata(), String.class)
        );
    }

    /**
     * Returns the name of the deadline.
     *
     * @return the name of the deadline
     */
    public String getDeadlineName() {
        return deadlineName;
    }

    /**
     * Returns the stored {@link ScopeDescriptor} of the deadline.
     *
     * @return the stored {@link ScopeDescriptor} of the deadline
     */
    public String getScopeDescriptor() {
        return scopeDescriptor;
    }

    /**
     * Returns the class name of the {@link ScopeDescriptor} of the deadline.
     *
     * @return the class name of the {@link ScopeDescriptor} of the deadline
     */
    public String getScopeDescriptorClass() {
        return scopeDescriptorClass;
    }

    /**
     * Returns the stored payload of the deadline.
     *
     * @return the stored payload of the deadline, if any
     */
    public @Nullable String getPayload() {
        return payload;
    }

    /**
     * Returns the type name of the payload of the deadline.
     *
     * @return the type name of the payload of the deadline
     */
    public @Nullable String getPayloadClass() {
        return payloadClass;
    }

    /**
     * Returns the revision of the payload type of the deadline.
     *
     * @return the revision of the payload type of the deadline, if any
     */
    public @Nullable String getPayloadRevision() {
        return payloadRevision;
    }

    /**
     * Returns the stored metadata of the deadline.
     *
     * @return the stored metadata of the deadline
     */
    public @Nullable String getMetaData() {
        return metaData;
    }

    /**
     * Converts the stored payload and metadata back into a {@link GenericDeadlineMessage}.
     *
     * @param converter the converter used to convert the stored payload and metadata
     * @return the {@link GenericDeadlineMessage} described by these details
     */
    public GenericDeadlineMessage asDeadLineMessage(StoredDeadlineConverter converter) {
        Object deserializedPayload = converter.payload(
                payloadClass == null ? StoredDeadlineConverter.EMPTY_TYPE : payloadClass, payloadRevision, payload
        );
        return new GenericDeadlineMessage(deadlineName,
                                          StoredDeadlineConverter.messageTypeOf(deserializedPayload),
                                          deserializedPayload,
                                          converter.metadata(metaData));
    }

    /**
     * Converts the stored {@link ScopeDescriptor} back into its class.
     *
     * @param converter the converter used to convert the stored {@link ScopeDescriptor}
     * @return the {@link ScopeDescriptor} described by these details
     */
    public ScopeDescriptor getDeserializedScopeDescriptor(StoredDeadlineConverter converter) {
        return converter.scope(scopeDescriptorClass, scopeDescriptor);
    }

    @Override
    public String toString() {
        return format("DbScheduler deadline details, deadlineName: [%s], " +
                              "scopeDescriptor: [%s], " +
                              "scopeDescriptorClass: [%s], " +
                              "payload: [%s], " +
                              "payloadClass: [%s], " +
                              "payloadRevision: [%s], " +
                              "metaData: [%s]",
                      deadlineName, scopeDescriptor, scopeDescriptorClass, payload, payloadClass, payloadRevision,
                      metaData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deadlineName, scopeDescriptor, scopeDescriptorClass, payload, payloadClass,
                            payloadRevision, metaData);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final DbSchedulerHumanReadableDeadlineDetails other = (DbSchedulerHumanReadableDeadlineDetails) obj;
        return Objects.equals(this.deadlineName, other.deadlineName) &&
                Objects.equals(this.scopeDescriptor, other.scopeDescriptor) &&
                Objects.equals(this.scopeDescriptorClass, other.scopeDescriptorClass) &&
                Objects.equals(this.payload, other.payload) &&
                Objects.equals(this.payloadClass, other.payloadClass) &&
                Objects.equals(this.payloadRevision, other.payloadRevision) &&
                Objects.equals(this.metaData, other.metaData);
    }
}
