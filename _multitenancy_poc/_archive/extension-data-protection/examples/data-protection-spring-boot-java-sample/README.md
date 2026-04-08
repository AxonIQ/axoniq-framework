# Axon Data Protection Extension - Spring Boot Sample

This sample application demonstrates how to use the Axon Data Protection extension for field-level encryption in event-sourced applications.

## What This Sample Demonstrates

- **Field-level encryption** with `@PersonalData` annotation
- **Nested object encryption** with `@DeepPersonalData` annotation
- **Serialized type encryption** with `@SerializedPersonalData` annotation
- **Key management** using JPA-based CryptoEngine
- **Automatic encryption/decryption** during event store operations
- **GDPR compliance** through cryptographic erasure (right to be forgotten)

## Prerequisites

- Java 21 or higher
- Maven 3.6 or higher
- Valid `axoniq.license` file (see [Licensing](#licensing))

## Project Structure

```
src/main/java/io/axoniq/dataprotection/sample/
├── DataProtectionSampleApplication.java      # Spring Boot main class
├── config/
│   └── DataProtectionConfiguration.java      # Encryption configuration
└── giftcard/
    └── event/
        ├── GiftCardIssuedEvent.java           # Event with all annotation types
        └── Address.java                       # Nested encrypted object
```

## Key Components

### DataProtectionConfiguration

Configures the encryption infrastructure:
- **CryptoEngine**: JPA-based key storage in PostgreSQL database
- **FieldEncryptingConverter**: Wraps Axon's converter for automatic encryption
- **FieldEncrypter**: Manual encryption/decryption when needed

### GiftCardIssuedEvent

Demonstrates all annotation types:

```java
public record GiftCardIssuedEvent(
    @DataSubjectId                          // Identifies the encryption key
    String giftCardId,

    @PersonalData                           // Type-preserving encryption
    String recipientName,

    @DeepPersonalData                       // Recursive nested object encryption
    Address shippingAddress,

    @SerializedPersonalData                 // Serialization + encryption
    LocalDate dateOfBirth,
    byte[] dateOfBirthEncrypted            // Storage field for serialized data
) {}
```

## Running the Sample

### 1. Start PostgreSQL

```bash
cd data-protection-spring-boot-sample
docker-compose up -d
```

This starts a PostgreSQL 16 container with:
- Database: `postgres`
- Username: `demo`
- Password: `demo`
- Port: `5432`

### 2. Build the Project

From the extension-data-protection root directory:

```bash
mvn clean install
```

### 3. Run the Application

```bash
cd data-protection-spring-boot-sample
mvn spring-boot:run
```

The application will start on `http://localhost:8080`.

### 4. Inspect the Database (Optional)

Connect to PostgreSQL to inspect the tables:

```bash
docker exec -it quickstart-postgres psql -U demo -d postgres
```

Then query the tables:
```sql
\c postgres
SET search_path TO dataprotection;
\dt

-- View encrypted events
SELECT * FROM aggregate_event_entry;

-- View encryption keys
SELECT * FROM axoniq_gdpr_keys;
```

## How Encryption Works

1. **Event Publication**: When a `GiftCardIssuedEvent` is created and stored:
   - `@DataSubjectId` field (`giftCardId`) determines which encryption key to use
   - `@PersonalData` fields (`recipientName`, `recipientEmail`) are encrypted in-place
   - `@DeepPersonalData` objects (`shippingAddress`) are recursively processed
   - `@SerializedPersonalData` fields (`dateOfBirth`) are serialized then encrypted

2. **Event Retrieval**: When the event is loaded from the event store:
   - All encrypted fields are automatically decrypted
   - Application code receives plain-text values
   - No manual encryption/decryption needed

3. **Cryptographic Erasure**: To delete a person's data:
   ```java
   cryptoEngine.deleteKey(giftCardId);
   ```
   - The encryption key is permanently deleted
   - Encrypted data becomes unreadable
   - Satisfies GDPR "right to be forgotten"

## Licensing

This sample requires a valid Axon Data Protection license file.

Place your `axoniq.license` file in one of these locations:
- Working directory: `./axoniq.license`
- Environment variable: `AXONIQ_DATAPROTECTION_LICENSE=/path/to/license`
- System property: `-Daxoniq.dataprotection.license=/path/to/license`
- Classpath: `src/main/resources/axoniq.license`

For license requests, contact: sales@axoniq.io

## Documentation

- [Axon Data Protection Reference Guide](../docs/reference/)
- [Axon Framework Documentation](https://docs.axoniq.io/)
- [GDPR Compliance Guide](../docs/reference/modules/ROOT/pages/index.adoc)

## Next Steps

To extend this sample:

1. **Add Commands**: Implement `IssueGiftCardCommand` and command handler
2. **Add Queries**: Create projection to query encrypted data
3. **Add REST API**: Expose endpoints for issuing cards and deleting data
4. **Try Different Crypto Engines**: Replace JPA with JDBC or HashiCorp Vault
5. **Implement Key Deletion**: Add endpoint to delete encryption keys

## License

Copyright (c) 2010-2025. AxonIQ B.V.

Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS
