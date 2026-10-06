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
import java.util.Arrays;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Pojo that contains the needed information for a {@link com.github.kagkarlsson.scheduler.task.Task} handling a
 * deadline, in binary form. Will be stored by db-scheduler's own serializer, Java serialization by default, as the data
 * of the task created by {@link DbSchedulerDeadlineManager#binaryTask(java.util.function.Supplier)}. The scope
 * descriptor, payload and metadata it holds are converted with the configured
 * {@link org.axonframework.conversion.Converter}.
 * <p>
 * The short field names and the {@code serialVersionUID} are those of Axon Framework 4.13, so that both versions read
 * each other's tasks.
 *
 * @author Gerard Klijs
 * @since 4.8.0
 */
@SuppressWarnings("Duplicates")
public class DbSchedulerBinaryDeadlineDetails implements Serializable {

    /**
     * The value Java serialization computed for the Axon Framework 4.13 version of this class, which declared none.
     */
    @Serial
    private static final long serialVersionUID = -3092788086374166433L;

    private String d;
    private byte[] s;
    private String sc;
    private byte @Nullable [] p;
    private @Nullable String pc;
    private @Nullable String r;
    private byte @Nullable [] m;

    @SuppressWarnings("NotNullFieldNotInitialized")
    DbSchedulerBinaryDeadlineDetails() {
        //no-args constructor needed for deserialization
    }

    /**
     * Creates a new {@code DbSchedulerBinaryDeadlineDetails} object from the stored form of a deadline's parts.
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
    public DbSchedulerBinaryDeadlineDetails(String deadlineName,
                                            byte[] scopeDescriptor,
                                            String scopeDescriptorClass,
                                            byte @Nullable [] payload,
                                            @Nullable String payloadClass,
                                            @Nullable String payloadRevision,
                                            byte @Nullable [] metaData) {
        this.d = deadlineName;
        this.s = scopeDescriptor;
        this.sc = scopeDescriptorClass;
        this.p = payload;
        this.pc = payloadClass;
        this.r = payloadRevision;
        this.m = metaData;
    }

    /**
     * Converts the given deadline into {@code DbSchedulerBinaryDeadlineDetails}.
     *
     * @param deadlineName the name of the deadline
     * @param descriptor   the {@link ScopeDescriptor} describing the scope of the deadline
     * @param message      the {@link DeadlineMessage} to store
     * @param converter    the converter used to convert the deadline's parts
     * @return the {@code DbSchedulerBinaryDeadlineDetails} of the given deadline
     */
    static DbSchedulerBinaryDeadlineDetails serialized(String deadlineName,
                                                       ScopeDescriptor descriptor,
                                                       DeadlineMessage message,
                                                       StoredDeadlineConverter converter) {
        Object payload = message.payload();
        return new DbSchedulerBinaryDeadlineDetails(deadlineName,
                                                    Objects.requireNonNull(converter.toStored(descriptor,
                                                                                              byte[].class)),
                                                    descriptor.getClass().getName(),
                                                    converter.toStored(payload, byte[].class),
                                                    StoredDeadlineConverter.typeNameOf(payload),
                                                    null,
                                                    converter.toStored(message.metadata(), byte[].class));
    }

    /**
     * Returns the name of the deadline.
     *
     * @return the name of the deadline
     */
    public String getD() {
        return d;
    }

    /**
     * Returns the stored {@link ScopeDescriptor} of the deadline.
     *
     * @return the stored {@link ScopeDescriptor} of the deadline
     */
    public byte[] getS() {
        return s;
    }

    /**
     * Returns the class name of the {@link ScopeDescriptor} of the deadline.
     *
     * @return the class name of the {@link ScopeDescriptor} of the deadline
     */
    public String getSc() {
        return sc;
    }

    /**
     * Returns the stored payload of the deadline.
     *
     * @return the stored payload of the deadline, if any
     */
    public byte @Nullable [] getP() {
        return p;
    }

    /**
     * Returns the type name of the payload of the deadline.
     *
     * @return the type name of the payload of the deadline
     */
    public @Nullable String getPc() {
        return pc;
    }

    /**
     * Returns the revision of the payload type of the deadline.
     *
     * @return the revision of the payload type of the deadline, if any
     */
    public @Nullable String getR() {
        return r;
    }

    /**
     * Returns the stored metadata of the deadline.
     *
     * @return the stored metadata of the deadline
     */
    public byte @Nullable [] getM() {
        return m;
    }

    /**
     * Converts the stored payload and metadata back into a {@link GenericDeadlineMessage}.
     *
     * @param converter the converter used to convert the stored payload and metadata
     * @return the {@link GenericDeadlineMessage} described by these details
     */
    public GenericDeadlineMessage asDeadLineMessage(StoredDeadlineConverter converter) {
        Object payload = converter.payload(pc == null ? StoredDeadlineConverter.EMPTY_TYPE : pc, r, p);
        return new GenericDeadlineMessage(d,
                                          StoredDeadlineConverter.messageTypeOf(payload),
                                          payload,
                                          converter.metadata(m));
    }

    /**
     * Converts the stored {@link ScopeDescriptor} back into its class.
     *
     * @param converter the converter used to convert the stored {@link ScopeDescriptor}
     * @return the {@link ScopeDescriptor} described by these details
     */
    public ScopeDescriptor getDeserializedScopeDescriptor(StoredDeadlineConverter converter) {
        return converter.scope(sc, s);
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
                      d, Arrays.toString(s), sc, Arrays.toString(p), pc, r, Arrays.toString(m));
    }

    @Override
    public int hashCode() {
        return Objects.hash(d, Arrays.hashCode(s), sc, Arrays.hashCode(p), pc, r, Arrays.hashCode(m));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final DbSchedulerBinaryDeadlineDetails other = (DbSchedulerBinaryDeadlineDetails) obj;
        return Objects.equals(this.d, other.d) &&
                Arrays.equals(this.s, other.s) &&
                Objects.equals(this.sc, other.sc) &&
                Arrays.equals(this.p, other.p) &&
                Objects.equals(this.pc, other.pc) &&
                Objects.equals(this.r, other.r) &&
                Arrays.equals(this.m, other.m);
    }
}
