/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jpa;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import org.jspecify.annotations.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The row of one running workflow in the {@link JpaWorkflowIndex}. Its segment key comes from the workflow id alone, so
 * it never changes when segments split or merge.
 *
 * @author Stefan Dragisic
 * @see io.axoniq.framework.workflow.runtime.execution.SegmentKeyRange
 * @since 5.5.0
 */
@Entity
@Table(indexes = @Index(columnList = "workflowModule,segmentKey"))
@IdClass(WorkflowIndexEntry.PK.class)
public class WorkflowIndexEntry {

    @Id
    @Column(length = 255)
    private String workflowModule;

    @Id
    @Column(length = 255)
    private String workflowId;

    @Basic(optional = false)
    private long segmentKey;

    // An ISO-8601 instant as text, like the timestamp of a token entry.
    private @Nullable String wakesAt;

    /**
     * Constructor required by JPA. Do not use.
     */
    protected WorkflowIndexEntry() {
    }

    /**
     * Creates the row of a running workflow.
     *
     * @param workflowModule the name of the workflow module the workflow runs in
     * @param workflowId     the identifier of the workflow
     * @param segmentKey     the segment key of the workflow id
     * @param wakesAt        the wake time as an ISO-8601 instant, or {@code null} when the workflow is not hibernated
     */
    public WorkflowIndexEntry(String workflowModule, String workflowId, long segmentKey, @Nullable String wakesAt) {
        this.workflowModule = Objects.requireNonNull(workflowModule, "The workflow module must not be null.");
        this.workflowId = Objects.requireNonNull(workflowId, "The workflow id must not be null.");
        this.segmentKey = segmentKey;
        this.wakesAt = wakesAt;
    }

    /**
     * Returns the name of the workflow module the workflow runs in.
     *
     * @return the name of the workflow module
     */
    public String getWorkflowModule() {
        return workflowModule;
    }

    /**
     * Returns the identifier of the workflow.
     *
     * @return the identifier of the workflow
     */
    public String getWorkflowId() {
        return workflowId;
    }

    /**
     * Returns the segment key of the workflow id.
     *
     * @return the segment key
     */
    public long getSegmentKey() {
        return segmentKey;
    }

    /**
     * Returns the time the workflow wakes at.
     *
     * @return the time as an ISO-8601 instant, or {@code null} when the workflow is not hibernated
     */
    public @Nullable String getWakesAt() {
        return wakesAt;
    }

    /**
     * The primary key of a {@link WorkflowIndexEntry}: the workflow module and the workflow id.
     */
    public static class PK implements Serializable {

        private String workflowModule;
        private String workflowId;

        /**
         * Constructor required by JPA. Do not use.
         */
        public PK() {
        }

        /**
         * Creates the key of the row of the given workflow.
         *
         * @param workflowModule the name of the workflow module
         * @param workflowId     the identifier of the workflow
         */
        public PK(String workflowModule, String workflowId) {
            this.workflowModule = workflowModule;
            this.workflowId = workflowId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PK that = (PK) o;
            return Objects.equals(workflowModule, that.workflowModule) && Objects.equals(workflowId, that.workflowId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workflowModule, workflowId);
        }
    }
}
