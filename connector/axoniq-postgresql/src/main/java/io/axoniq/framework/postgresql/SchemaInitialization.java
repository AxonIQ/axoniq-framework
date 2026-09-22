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

package io.axoniq.framework.postgresql;

/**
 * Controls whether and how {@link PostgresqlEventStorageEngine} ensures its PostgreSQL schema (tables, sequence,
 * indices, and internal migrations) is present when it is constructed.
 * <p>
 * Writing events against a schema that is not fully up to date can corrupt data in ways that are hard to recover
 * from -- an incomplete schema can silently produce corrupted or incomplete data instead of failing outright. The
 * options below trade off startup cost and self-healing behavior against that risk.
 *
 * @author John Hendrikx
 * @since 5.4.0
 */
public enum SchemaInitialization {

    /**
     * Checks whether the schema is complete, and creates or repairs whatever is missing (tables, sequence,
     * indices, and internal migrations). This is the default.
     * <p>
     * When the schema is already complete, this only performs cheap existence checks and does not lock any of
     * the event store's tables. When something is missing, the (idempotent) creation statements run, which do
     * briefly lock the affected tables.
     */
    CREATE_IF_MISSING,

    /**
     * Checks whether the schema is complete, and fails construction with an {@link IllegalStateException} if it
     * is not. Never creates or modifies anything.
     * <p>
     * Use this when schema changes are managed out-of-band (e.g. by a dedicated migration step or a database
     * administrator) and an incomplete schema should be treated as a startup failure rather than something the
     * application silently repairs.
     */
    VALIDATE,

    /**
     * Skips all schema checks. The schema is assumed to already be complete and correct.
     * <p>
     * This avoids any database round trip or locking at construction time, but writing events against a schema
     * that is not actually complete can corrupt data, as described in this enum's class-level documentation.
     * Only use this once the schema is known to be up to date, for example after a rolling deployment where
     * earlier instances have already migrated it.
     */
    SKIP
}
