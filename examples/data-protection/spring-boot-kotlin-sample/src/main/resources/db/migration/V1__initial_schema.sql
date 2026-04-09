-- Create schema
CREATE SCHEMA IF NOT EXISTS dataprotection;

-- Axon Framework 5 aggregate event store table
-- This table stores all events for event sourcing using Axon's Dynamic Consistency Boundary (DCB) approach
CREATE TABLE IF NOT EXISTS dataprotection.aggregate_event_entry (
    global_index                BIGSERIAL PRIMARY KEY,
    aggregate_type              VARCHAR(255),
    aggregate_identifier        VARCHAR(255) NOT NULL,
    aggregate_sequence_number   BIGINT,
    type                        VARCHAR(255) NOT NULL,
    version                     VARCHAR(255) NOT NULL,
    timestamp                   VARCHAR(255) NOT NULL,
    payload                     OID NOT NULL,
    metadata                    OID,
    identifier                  VARCHAR(255)
);

-- Create sequence for global index (required by JPA @SequenceGenerator)
CREATE SEQUENCE IF NOT EXISTS dataprotection."aggregate-event-global-index-sequence" START WITH 1 INCREMENT BY 1;

-- Create unique index on aggregate identifier and sequence number
-- This ensures event ordering consistency within an aggregate
CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregate_event_entry_aggregate
    ON dataprotection.aggregate_event_entry(aggregate_identifier, aggregate_sequence_number);

-- Create index on identifier for event lookup
CREATE INDEX IF NOT EXISTS idx_aggregate_event_entry_identifier
    ON dataprotection.aggregate_event_entry(identifier);

-- Axon Framework 5 token store table for Streaming Event Processors
-- This table stores tracking tokens for event processing progress
CREATE TABLE IF NOT EXISTS dataprotection.token_entry (
    processor_name  VARCHAR(255) NOT NULL,
    segment         INT NOT NULL,
    mask            INT NOT NULL,
    token           OID,
    token_type      VARCHAR(255),
    timestamp       VARCHAR(255) NOT NULL,
    owner           VARCHAR(255),
    PRIMARY KEY (processor_name, segment)
);

-- Data Protection - Encryption Keys table
CREATE TABLE IF NOT EXISTS dataprotection.axoniq_gdpr_keys (
    key_id VARCHAR(255) NOT NULL,
    secret_key TEXT NOT NULL,
    PRIMARY KEY (key_id)
);

CREATE INDEX IF NOT EXISTS idx_axoniq_gdpr_keys_key_id
    ON dataprotection.axoniq_gdpr_keys (key_id);

-- Create gift_card projection table
CREATE TABLE IF NOT EXISTS dataprotection.gift_card (
    gift_card_id            VARCHAR(255) NOT NULL,
    remaining_value         NUMERIC(19, 2) NOT NULL,
    initial_value           NUMERIC(19, 2) NOT NULL,
    username                TEXT,
    -- Person owner stored as JSONB
    owner                   JSONB,
    -- SerializedPersonalData fields
    date_of_birth           DATE,
    random_number           INTEGER,
    creation_date           TIMESTAMP WITH TIME ZONE NOT NULL,
    last_modified_date      TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (gift_card_id)
);

-- Create index on username for faster queries (even though it's encrypted)
CREATE INDEX IF NOT EXISTS idx_gift_card_username ON dataprotection.gift_card(username);

-- Create index on creation_date for ordering
CREATE INDEX IF NOT EXISTS idx_gift_card_creation_date ON dataprotection.gift_card(creation_date DESC);
