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

import org.axonframework.common.annotation.Internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * Creates the tables, sequence, and indices that {@link PostgresqlEventStorageEngine} relies on, if
 * they do not already exist, or validates their presence, depending on the requested
 * {@link SchemaInitialization}.
 *
 * @author John Hendrikx
 * @since 5.2.0
 */
@Internal
final class PostgresqlSchemaInitializer {

    private PostgresqlSchemaInitializer() {
        // utility class
    }

    /**
     * Query returning a single boolean column: whether every table, sequence, index, and internal migration that
     * {@link PostgresqlEventStorageEngine} relies on is present. Uses {@code to_regclass}, which returns
     * {@code NULL} (rather than raising an error) for a relation that does not exist, so this is safe to run
     * against a database that has none of the schema yet.
     */
    private static final String SCHEMA_COMPLETE_QUERY =
        """
        SELECT to_regclass('events') IS NOT NULL
           AND to_regclass('tags') IS NOT NULL
           AND to_regclass('consistency_tags') IS NOT NULL
           AND to_regclass('events_monotonic_seq') IS NOT NULL
           AND to_regclass('consistency_tags_global_index_idx') IS NOT NULL
           AND to_regclass('tags_global_index_idx') IS NOT NULL
           AND to_regclass('tags_type_unique') IS NOT NULL
           AND EXISTS (
             SELECT 1 FROM pg_trigger
             WHERE tgname = 'axon_events_write_type_tag' AND tgrelid = to_regclass('events')
           )
           AND EXISTS (
             SELECT 1 FROM pg_trigger
             WHERE tgname = 'axon_tags_validate_type' AND tgrelid = to_regclass('tags')
           )
        """;

    /**
     * Ensures the schema {@link PostgresqlEventStorageEngine} relies on is present, per {@code schemaInitialization}.
     *
     * @param dataSource            a data source to connect to PostgreSQL, cannot be {@code null}
     * @param schemaInitialization  how to handle a missing or incomplete schema, cannot be {@code null}
     * @throws SQLException          when a JDBC error occurred
     * @throws IllegalStateException when {@code schemaInitialization} is {@link SchemaInitialization#VALIDATE} and
     *                                the schema is not complete
     */
    static void initialize(DataSource dataSource, SchemaInitialization schemaInitialization) throws SQLException {
        switch (schemaInitialization) {
            case SKIP -> {
                // Trust the caller: no database round trip at all.
            }
            case VALIDATE -> {
                if (!schemaIsComplete(dataSource)) {
                    throw new IllegalStateException(
                        "The PostgreSQL schema for " + PostgresqlEventStorageEngine.class.getSimpleName()
                        + " is missing one or more tables, indices, or internal migrations, and "
                        + SchemaInitialization.class.getSimpleName() + ".VALIDATE does not create or repair "
                        + "schema objects. Apply the missing schema manually, or use "
                        + SchemaInitialization.class.getSimpleName() + ".CREATE_IF_MISSING instead."
                    );
                }
            }
            case CREATE_IF_MISSING -> {
                if (!schemaIsComplete(dataSource)) {
                    createSchema(dataSource);
                    installTypeTagMigration(dataSource);
                }
            }
        }
    }

    /**
     * Checks whether every table, sequence, index, and internal migration that
     * {@link PostgresqlEventStorageEngine} relies on is present. Only issues cheap catalog look-ups; never
     * acquires a lock on any of the schema's own tables.
     *
     * @param dataSource a data source to connect to PostgreSQL, cannot be {@code null}
     * @return {@code true} if the schema is complete, {@code false} otherwise
     * @throws SQLException when a JDBC error occurred
     */
    static boolean schemaIsComplete(DataSource dataSource) throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SCHEMA_COMPLETE_QUERY)
        ) {
            resultSet.next();

            return resultSet.getBoolean(1);
        }
    }

    // TODO #7 Allow to configure tables, sequences and indices
    static void createSchema(DataSource dataSource) throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
        ) {
            connection.setAutoCommit(false);

            statement.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                  global_index INT8 NOT NULL GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY -1),

                  timestamp TIMESTAMPTZ NOT NULL,
                  payload BYTEA,
                  metadata JSON NOT NULL,
                  identifier VARCHAR NOT NULL,
                  type VARCHAR NOT NULL,
                  type_version VARCHAR NOT NULL,

                  -- keys
                  PRIMARY KEY (global_index)
                );

                CREATE TABLE IF NOT EXISTS tags (
                  global_index INT8 NOT NULL,

                  key VARCHAR NOT NULL,
                  value VARCHAR NOT NULL,

                  -- keys
                  PRIMARY KEY (key, value, global_index)
                );

                CREATE TABLE IF NOT EXISTS consistency_tags (
                  tag_hash INT4 NOT NULL,
                  global_index INT8 NOT NULL,

                  -- keys
                  PRIMARY KEY (tag_hash)
                );

                -- Create a sequence used for monotonic final global index values. Starts at 1.
                CREATE SEQUENCE IF NOT EXISTS events_monotonic_seq
                  INCREMENT BY 1
                  CACHE 1
                  OWNED BY events.global_index;

                -- BTREE index on global_index in consistency_tags (for faster finalizations)
                CREATE INDEX IF NOT EXISTS consistency_tags_global_index_idx
                  ON consistency_tags (global_index);

                -- BTREE index on global_index in tags (for faster finalizations)
                CREATE INDEX IF NOT EXISTS tags_global_index_idx
                  ON tags (global_index);

                -- Ensures at most one reserved type tag row per event
                CREATE UNIQUE INDEX IF NOT EXISTS tags_type_unique
                  ON tags (global_index) WHERE key = '__T';
                """
            );

            connection.commit();
        }
    }

    /**
     * Older schemas (from before "type_version" existed) stored an event's full {@code MessageType}
     * String representation - "qualifiedName#version" (see {@code MessageType#toString()}) - in a
     * single "type" column. This migration splits any such pre-existing values into "type" (holding
     * only the qualified name from now on) and the new "type_version" column, which is required
     * ({@code NOT NULL}). This makes the whole migration a hard cut-over, not something older,
     * unaware application instances can keep running against: once it runs, any instance whose
     * insert into "events" does not supply "type_version" fails outright, since there is no default
     * for it to fall back on. Every instance appending events must already be upgraded before (or,
     * practically, around the same time as) this migration runs.
     *
     * The reserved type tag (see PostgresqlEventStorageEngine's TYPE_TAG_KEY) is then populated from
     * the now-split "type" column directly. A trigger writes it alongside every event's regular tags,
     * allowing type to be filtered using the same mechanism as any other tag. This runs as a database
     * trigger rather than application code so it applies uniformly to every insert into "events" -
     * including any current or future insertion code path within the (now uniformly upgraded)
     * application - without any of them needing to know about the reserved tag themselves.
     *
     * A second trigger independently validates that every "__T" row's value matches its referenced
     * event's type, regardless of which code path wrote it - a guarantee that doesn't depend on the
     * write-side trigger (or anything else) staying correct.
     *
     * The one-time historical backfill (for events that already existed before this trigger was
     * first installed) runs atomically alongside both triggers' creation, in the same transaction.
     * Once this block commits, no event can ever be missing its type tag again - which is also why
     * it is safe to skip this block entirely once the trigger is present.
     *
     * @param dataSource a data source to connect to PostgreSQL, cannot be {@code null}
     * @throws SQLException when a JDBC error occurred
     */
    static void installTypeTagMigration(DataSource dataSource) throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
        ) {
            try (ResultSet resultSet = statement.executeQuery(
                "SELECT 1 FROM pg_trigger WHERE tgname = 'axon_events_write_type_tag' AND tgrelid = 'events'::regclass"
            )) {
                if (resultSet.next()) {
                    return;  // migration already installed
                }
            }

            connection.setAutoCommit(false);

            statement.execute(
                """
                -- Adds "type_version" if it does not already exist (a fresh schema already has it -
                -- see createSchema()) and splits any pre-existing "type" values of the form
                -- "qualifiedName#version" into the two columns.
                ALTER TABLE events ADD COLUMN IF NOT EXISTS type_version VARCHAR;

                UPDATE events
                  SET type_version = split_part(type, '#', 2),
                      type = split_part(type, '#', 1)
                  WHERE type_version IS NULL;

                ALTER TABLE events ALTER COLUMN type_version SET NOT NULL;

                -- Writes the reserved "__T" tag with the event's type alongside its regular tags.
                CREATE OR REPLACE FUNCTION axon_write_type_tag() RETURNS TRIGGER AS $$
                BEGIN
                  INSERT INTO tags (global_index, key, value) VALUES (NEW.global_index, '__T', NEW.type)
                    ON CONFLICT (global_index) WHERE key = '__T' DO UPDATE SET value = EXCLUDED.value;
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER axon_events_write_type_tag
                  AFTER INSERT ON events
                  FOR EACH ROW EXECUTE FUNCTION axon_write_type_tag();

                -- Guarantees value equals the referenced event's type for every "__T" row.
                CREATE OR REPLACE FUNCTION axon_validate_type_tag() RETURNS TRIGGER AS $$
                BEGIN
                  IF NEW.key = '__T' AND NOT EXISTS (
                    SELECT 1 FROM events WHERE global_index = NEW.global_index AND type = NEW.value
                  ) THEN
                    RAISE EXCEPTION 'Type tag value % does not match event type for global_index %', NEW.value, NEW.global_index;
                  END IF;
                  RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER axon_tags_validate_type
                  BEFORE INSERT OR UPDATE ON tags
                  FOR EACH ROW EXECUTE FUNCTION axon_validate_type_tag();

                -- Backfills "__T" for events that already existed before this migration ran.
                INSERT INTO tags (global_index, key, value)
                  SELECT global_index, '__T', type FROM events;
                """
            );

            connection.commit();
        }
    }
}
