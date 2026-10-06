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

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.deadline.StoredDeadlineConverter;
import org.axonframework.messaging.ScopeDescriptor;
import org.jspecify.annotations.Nullable;

/**
 * Wraps the details of a deadline scheduled through the {@link JobRunrDeadlineManager}: the deadline's name, and its
 * scope descriptor, payload and metadata in their stored form. The details are themselves converted into a
 * {@code String}, which JobRunr stores as the argument of the job that fires the deadline.
 * <p>
 * The field names are those of Axon Framework 4.13, so that both versions read each other's jobs.
 *
 * @author Tom de Backer
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class DeadlineDetails {

    private String deadlineName;
    private String scopeDescriptor;
    private String scopeDescriptorClass;
    private @Nullable String payload;
    private @Nullable String payloadClass;
    private @Nullable String payloadRevision;
    private @Nullable String metaData;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private DeadlineDetails() {
        //private no-args constructor needed for deserialization
    }

    /**
     * Creates a new {@code DeadlineDetails} object from the stored form of a deadline's parts.
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
    public DeadlineDetails(String deadlineName,
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
     * Converts the given deadline into {@code DeadlineDetails}, and those into their stored {@code String} form.
     *
     * @param deadlineName the name of the deadline
     * @param descriptor   the {@link ScopeDescriptor} describing the scope of the deadline
     * @param message      the {@link DeadlineMessage} to store
     * @param converter    the converter used to convert the deadline's parts and the details themselves
     * @return the stored form of the deadline's details
     */
    static String serialized(String deadlineName,
                             ScopeDescriptor descriptor,
                             DeadlineMessage message,
                             StoredDeadlineConverter converter) {
        Object payload = message.payload();
        DeadlineDetails deadlineDetails = new DeadlineDetails(
                deadlineName,
                converter.toStored(descriptor, String.class),
                descriptor.getClass().getName(),
                converter.toStored(payload, String.class),
                StoredDeadlineConverter.typeNameOf(payload),
                null,
                converter.toStored(message.metadata(), String.class)
        );
        return converter.toStored(deadlineDetails, String.class);
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
        Object deserializedPayload = converter.payload(payloadClass == null ? StoredDeadlineConverter.EMPTY_TYPE
                                                                            : payloadClass,
                                                       payloadRevision,
                                                       payload);
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
}
