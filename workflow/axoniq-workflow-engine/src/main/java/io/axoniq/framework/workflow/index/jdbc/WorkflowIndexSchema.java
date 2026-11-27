/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.index.jdbc;

import java.util.Objects;

/**
 * The table and column names of a {@link JdbcWorkflowIndex}. The defaults are the names the
 * {@link io.axoniq.framework.workflow.index.jpa.JpaWorkflowIndex} maps with the default JPA naming, so both write the
 * same tables. Under a naming strategy that renames them, such as the default of Spring Boot, configure the names it
 * produces.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
public class WorkflowIndexSchema {

    private final String workflowTable;
    private final String waitTable;
    private final String initializationTable;
    private final String positionTable;
    private final String workflowModuleColumn;
    private final String workflowIdColumn;
    private final String segmentKeyColumn;
    private final String wakesAtColumn;
    private final String waitIdColumn;
    private final String stepNameColumn;
    private final String eventNameColumn;
    private final String associationsColumn;
    private final String segmentFromColumn;
    private final String segmentToColumn;
    private final String segmentPositionColumn;

    /**
     * Creates a schema from the given builder.
     *
     * @param builder the builder holding the table and column names
     */
    protected WorkflowIndexSchema(Builder builder) {
        this.workflowTable = builder.workflowTable;
        this.waitTable = builder.waitTable;
        this.initializationTable = builder.initializationTable;
        this.positionTable = builder.positionTable;
        this.workflowModuleColumn = builder.workflowModuleColumn;
        this.workflowIdColumn = builder.workflowIdColumn;
        this.segmentKeyColumn = builder.segmentKeyColumn;
        this.wakesAtColumn = builder.wakesAtColumn;
        this.waitIdColumn = builder.waitIdColumn;
        this.stepNameColumn = builder.stepNameColumn;
        this.eventNameColumn = builder.eventNameColumn;
        this.associationsColumn = builder.associationsColumn;
        this.segmentFromColumn = builder.segmentFromColumn;
        this.segmentToColumn = builder.segmentToColumn;
        this.segmentPositionColumn = builder.segmentPositionColumn;
    }

    /**
     * Returns a builder whose names default to those of the JPA mapping.
     *
     * @return a builder of a {@link WorkflowIndexSchema}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the schema with the default names, those of the JPA mapping.
     *
     * @return the default schema
     */
    public static WorkflowIndexSchema defaultSchema() {
        return builder().build();
    }

    /**
     * Returns the name of the table with a row per running workflow.
     *
     * @return the configured name, {@code WorkflowIndexEntry} by default
     */
    public String workflowTable() {
        return workflowTable;
    }

    /**
     * Returns the name of the table with a row per open wait.
     *
     * @return the configured name, {@code WorkflowIndexWaitEntry} by default
     */
    public String waitTable() {
        return waitTable;
    }

    /**
     * Returns the name of the table with a row per initialized workflow module.
     *
     * @return the configured name, {@code WorkflowIndexInitialization} by default
     */
    public String initializationTable() {
        return initializationTable;
    }

    /**
     * Returns the name of the table with the token position of each segment.
     *
     * @return the configured name, {@code WorkflowIndexSegmentEntry} by default
     */
    public String positionTable() {
        return positionTable;
    }

    /**
     * Returns the name of the column with the name of the workflow module, in every table.
     *
     * @return the configured name, {@code workflowModule} by default
     */
    public String workflowModuleColumn() {
        return workflowModuleColumn;
    }

    /**
     * Returns the name of the column with the workflow id, in the workflow and wait tables.
     *
     * @return the configured name, {@code workflowId} by default
     */
    public String workflowIdColumn() {
        return workflowIdColumn;
    }

    /**
     * Returns the name of the column with the segment key, in the workflow and wait tables.
     *
     * @return the configured name, {@code segmentKey} by default
     */
    public String segmentKeyColumn() {
        return segmentKeyColumn;
    }

    /**
     * Returns the name of the column with the time a hibernated workflow wakes at.
     *
     * @return the configured name, {@code wakesAt} by default
     */
    public String wakesAtColumn() {
        return wakesAtColumn;
    }

    /**
     * Returns the name of the column with the identifier of a wait row.
     *
     * @return the configured name, {@code id} by default
     */
    public String waitIdColumn() {
        return waitIdColumn;
    }

    /**
     * Returns the name of the column with the name of the wait step.
     *
     * @return the configured name, {@code stepName} by default
     */
    public String stepNameColumn() {
        return stepNameColumn;
    }

    /**
     * Returns the name of the column with the name of the awaited event.
     *
     * @return the configured name, {@code eventName} by default
     */
    public String eventNameColumn() {
        return eventNameColumn;
    }

    /**
     * Returns the name of the column with the converted associations of a wait.
     *
     * @return the configured name, {@code associations} by default
     */
    public String associationsColumn() {
        return associationsColumn;
    }

    /**
     * Returns the name of the column with the lowest segment key of a segment, in the position table.
     *
     * @return the configured name, {@code segmentFrom} by default
     */
    public String segmentFromColumn() {
        return segmentFromColumn;
    }

    /**
     * Returns the name of the column with the highest segment key of a segment, in the position table.
     *
     * @return the configured name, {@code segmentTo} by default
     */
    public String segmentToColumn() {
        return segmentToColumn;
    }

    /**
     * Returns the name of the column with the token position of a segment.
     *
     * @return the configured name, {@code segmentPosition} by default
     */
    public String segmentPositionColumn() {
        return segmentPositionColumn;
    }

    /**
     * Builds a {@link WorkflowIndexSchema}. Every name defaults to that of the JPA mapping.
     */
    public static class Builder {

        private String workflowTable = "WorkflowIndexEntry";
        private String waitTable = "WorkflowIndexWaitEntry";
        private String initializationTable = "WorkflowIndexInitialization";
        private String positionTable = "WorkflowIndexSegmentEntry";
        private String workflowModuleColumn = "workflowModule";
        private String workflowIdColumn = "workflowId";
        private String segmentKeyColumn = "segmentKey";
        private String wakesAtColumn = "wakesAt";
        private String waitIdColumn = "id";
        private String stepNameColumn = "stepName";
        private String eventNameColumn = "eventName";
        private String associationsColumn = "associations";
        private String segmentFromColumn = "segmentFrom";
        private String segmentToColumn = "segmentTo";
        private String segmentPositionColumn = "segmentPosition";

        /**
         * Sets the name of the table with a row per running workflow. Defaults to {@code WorkflowIndexEntry}.
         *
         * @param workflowTable the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder workflowTable(String workflowTable) {
            this.workflowTable = Objects.requireNonNull(workflowTable, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the table with a row per open wait. Defaults to {@code WorkflowIndexWaitEntry}.
         *
         * @param waitTable the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder waitTable(String waitTable) {
            this.waitTable = Objects.requireNonNull(waitTable, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the table with a row per initialized workflow module.
         * Defaults to {@code WorkflowIndexInitialization}.
         *
         * @param initializationTable the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder initializationTable(String initializationTable) {
            this.initializationTable = Objects.requireNonNull(initializationTable, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the table with the token position of each segment.
         * Defaults to {@code WorkflowIndexSegmentEntry}.
         *
         * @param positionTable the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder positionTable(String positionTable) {
            this.positionTable = Objects.requireNonNull(positionTable, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the name of the workflow module, in every table.
         * Defaults to {@code workflowModule}.
         *
         * @param workflowModuleColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder workflowModuleColumn(String workflowModuleColumn) {
            this.workflowModuleColumn = Objects.requireNonNull(workflowModuleColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the workflow id, in the workflow and wait tables.
         * Defaults to {@code workflowId}.
         *
         * @param workflowIdColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder workflowIdColumn(String workflowIdColumn) {
            this.workflowIdColumn = Objects.requireNonNull(workflowIdColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the segment key, in the workflow and wait tables.
         * Defaults to {@code segmentKey}.
         *
         * @param segmentKeyColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder segmentKeyColumn(String segmentKeyColumn) {
            this.segmentKeyColumn = Objects.requireNonNull(segmentKeyColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the time a hibernated workflow wakes at. Defaults to {@code wakesAt}.
         *
         * @param wakesAtColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder wakesAtColumn(String wakesAtColumn) {
            this.wakesAtColumn = Objects.requireNonNull(wakesAtColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the identifier of a wait row. Defaults to {@code id}.
         *
         * @param waitIdColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder waitIdColumn(String waitIdColumn) {
            this.waitIdColumn = Objects.requireNonNull(waitIdColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the name of the wait step. Defaults to {@code stepName}.
         *
         * @param stepNameColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder stepNameColumn(String stepNameColumn) {
            this.stepNameColumn = Objects.requireNonNull(stepNameColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the name of the awaited event. Defaults to {@code eventName}.
         *
         * @param eventNameColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder eventNameColumn(String eventNameColumn) {
            this.eventNameColumn = Objects.requireNonNull(eventNameColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the converted associations of a wait. Defaults to {@code associations}.
         *
         * @param associationsColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder associationsColumn(String associationsColumn) {
            this.associationsColumn = Objects.requireNonNull(associationsColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the lowest segment key of a segment, in the position table.
         * Defaults to {@code segmentFrom}.
         *
         * @param segmentFromColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder segmentFromColumn(String segmentFromColumn) {
            this.segmentFromColumn = Objects.requireNonNull(segmentFromColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the highest segment key of a segment, in the position table.
         * Defaults to {@code segmentTo}.
         *
         * @param segmentToColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder segmentToColumn(String segmentToColumn) {
            this.segmentToColumn = Objects.requireNonNull(segmentToColumn, "The name must not be null.");
            return this;
        }

        /**
         * Sets the name of the column with the token position of a segment. Defaults to {@code segmentPosition}.
         *
         * @param segmentPositionColumn the name to use
         * @return this builder, for fluent interfacing
         */
        public Builder segmentPositionColumn(String segmentPositionColumn) {
            this.segmentPositionColumn = Objects.requireNonNull(segmentPositionColumn, "The name must not be null.");
            return this;
        }

        /**
         * Builds the schema.
         *
         * @return a schema with the configured names
         */
        public WorkflowIndexSchema build() {
            return new WorkflowIndexSchema(this);
        }
    }
}
