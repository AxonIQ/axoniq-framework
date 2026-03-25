# AxonIQ Framework - Data Protection Extension

This repository provides an extension to the [Axon Framework](https://github.com/AxonFramework/AxonFramework), offering
field-level encryption and key management capabilities for event-sourced applications. It enables GDPR compliance through
cryptographic erasure (the "Right to be Forgotten") by encrypting sensitive fields with deletable keys.

For more information on anything Axon, please visit our website, [http://axoniq.io](http://axoniq.io).

## Overview

The core capability of Axon Data Protection is to encrypt and decrypt individual fields of a Java object using a key
determined by the value of another field of the object. The main use case is to enable effective deletion of encrypted
field contents by destroying the encryption key.

### The Problem

With event sourcing, all data is stored as a series of immutable, undeletable events. This seems to preclude erasing
any data, which conflicts with GDPR's "Right to be Forgotten" mandate. While you could modify immutable events anyway,
this suffers from significant drawbacks and potential operational costs, and damages the integrity of the event stream
as an audit record.

### The Solution

By encrypting parts of event data and storing encryption keys outside the event stream (where they can be deleted),
data erasure becomes possible: **deleting the key effectively deletes the encrypted data permanently**, as it becomes
unrecoverable without the key.

## Getting started

### Prerequisites

* Java 21 or higher
* Axon Framework 5.x
* A valid `axoniq.license` file (contact sales@axoniq.io if you don't have one)

### License File

The Data Protection Extension is commercial software provided by AxonIQ B.V. and requires a license file named
`axoniq.license`.

The license file will be automatically picked up if it's in the working directory. Otherwise, specify its location through:
- Environment variable: `AXONIQ_DATAPROTECTION_LICENSE`
- System property: `axoniq.dataprotection.license`

Alternatively, place the license file on the classpath as a resource for easier deployment.

### Installation

Add the dependency to your project:

```xml
<dependency>
    <groupId>io.axoniq.framework.extension.dataprotection</groupId>
    <artifactId>axon-data-protection</artifactId>
    <version>5.0.0-SNAPSHOT</version>
</dependency>
```

Note: The extension has an _optional_ dependency on Axon Framework to avoid overriding your project's Axon version.
You must explicitly include Axon Framework 5.x in your dependencies.

### Basic Configuration (Spring Boot)

Configure the Data Protection Extension with a JPA-based key store:

```java
@Bean
public CryptoEngine cryptoEngine(EntityManagerFactory entityManagerFactory) {
    return new JpaCryptoEngine(entityManagerFactory);
}

@Bean("defaultAxonObjectMapper")
@ConditionalOnMissingBean(name = "defaultAxonObjectMapper")
public ObjectMapper defaultAxonObjectMapper() {
    return new ObjectMapper();
}

@Bean(name = "converter")
@Primary
@ConditionalOnMissingBean(Converter.class)
public Converter converter(CryptoEngine cryptoEngine,
                           @Qualifier("defaultAxonObjectMapper") ObjectMapper objectMapper) {
    ChainingContentTypeConverter contentTypeConverter = new ChainingContentTypeConverter(
            getClass().getClassLoader()
    );
    Converter delegateConverter = new JacksonConverter(objectMapper, contentTypeConverter);
    return new FieldEncryptingConverter(cryptoEngine, delegateConverter);
}

@Bean
public FieldEncrypter fieldEncrypter(CryptoEngine cryptoEngine, Converter converter) {
    return new FieldEncrypter(cryptoEngine, converter);
}
```

This configuration:
* Manages encryption keys in your database via JPA
* Wraps the default Converter to automatically encrypt/decrypt annotated fields
* Makes encryption/decryption transparent to your application code

### Annotate Your Events

Mark fields for encryption using Axon Data Protection annotations:

```java
public class CustomerCreatedEvent {

    @DataSubjectId
    private UUID customerId;

    @PersonalData
    private String name;

    @PersonalData
    private String email;

    // constructors, getters, etc.
}
```

**Annotations explained:**

* `@DataSubjectId` - Marks the field that identifies the encryption key to use. The field's `toString()` value becomes
  the key identifier.
* `@PersonalData` - Marks fields for type-preserving encryption. Supports `String`, `byte[]`, arrays, `Collection`,
  `Map`, and Scala `Option`.
* `@SerializedPersonalData` - For fields that require serialization before encryption (e.g., `LocalDate`). Requires a
  separate storage field:

```java
@SerializedPersonalData
LocalDate dateOfBirth;
byte[] dateOfBirthEncrypted;  // Storage field (automatically detected by naming convention)
```

* `@DeepPersonalData` - Recursively processes nested objects for encryption annotations:

```java
public class Person {
    @DataSubjectId
    UUID id;

    @PersonalData
    String name;

    @DeepPersonalData
    Address address;  // Address class may have @PersonalData fields
}
```

### Java Records Support (New in 5.0)

Full support for Java records with all annotations:

```java
public record PersonRegisteredEvent(
    @DataSubjectId UUID id,
    @PersonalData String name,
    @PersonalData byte[] picture,
    String city
) {}
```

Records are immutable, so the extension creates new record instances with modified field values during
encryption/decryption.

### Implement Right to be Forgotten

To delete personal data, simply delete the encryption key:

```java
@CommandHandler
public void handle(DeleteCustomerDataCommand command) {
    // Delete the key - this makes all encrypted fields unrecoverable
    cryptoEngine.deleteKey(customerId.toString());

    // Apply event for query model cleanup
    apply(new CustomerDataDeletedEvent(customerId));
}
```

After key deletion:
- Encrypted fields in events become permanently unrecoverable
- On deserialization, fields are replaced with default values (configurable via `ReplacementValueProvider`)
- Query models should handle `CustomerDataDeletedEvent` to delete/anonymize read-side data

## Advanced Features

### Multiple Keys per Object (Groups)

Define multiple encryption keys within the same object using the `group` attribute:

```java
public class CustomerEvent {
    @DataSubjectId(group = "name", prefix = "name-")
    @DataSubjectId(group = "address", prefix = "address-")
    private int customerId;

    @PersonalData(group = "name")
    private String name;

    @PersonalData(group = "address")
    private String address;
}
```

If `customerId` is `913`, two keys will be created: `name-913` and `address-913`. You can delete them individually
to selectively erase specific field groups.

### Custom Replacement Values

Provide custom values when keys are deleted:

```java
public class YearOnlyReplacementValueProvider extends ReplacementValueProvider {
    @Override
    public Object replacementValue(Class<?> clazz, Field field,
            Type fieldType, String groupName, String replacement,
            byte[] storedPartialValue) {
        if (LocalDate.class.equals(fieldType) &&
                "*YEARONLY*".equals(replacement) &&
                storedPartialValue != null) {
            ByteBuffer buffer = ByteBuffer.wrap(storedPartialValue);
            return LocalDate.of(buffer.getInt(), Month.JANUARY, 1);
        }
        return super.replacementValue(clazz, field, fieldType, groupName, replacement, storedPartialValue);
    }

    @Override
    public byte[] partialValueForStorage(Class<?> clazz, Field field,
            Type fieldType, String groupName, String replacement,
            Object inputValue) {
        if (LocalDate.class.equals(fieldType) &&
                "*YEARONLY*".equals(replacement) &&
                inputValue instanceof LocalDate) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(((LocalDate)inputValue).getYear());
            return buffer.array();
        }
        return super.partialValueForStorage(clazz, field, fieldType, groupName, replacement, inputValue);
    }
}
```

Use case: Delete full date of birth but keep the year for analytics.

### Preset Encryption Contexts

For system-wide keys (not derived from object fields):

```java
// Single key for default group
fieldEncrypter.encrypt(object, "system-wide-key");

// Multiple keys for different groups
Map<String, String> keys = Map.of(
    "", "default-key",           // Default group
    "sensitive", "sensitive-key" // Named group
);
fieldEncrypter.encrypt(object, keys);
```

### Partial Encryption

Process only specific groups:

```java
// Encrypt only the "sensitive" group
Set<String> groups = Set.of("sensitive");
fieldEncrypter.encrypt(object, groups);
```

### Scala Support

Scala applications can use lower camel case type aliases:

```scala
case class NewCustomerEvent(
    @dataSubjectId id: Int,
    @personalData name: String
)
```

Supports Scala collections (`Option`, `Iterable`, `Map`) and Scala 2.13.

## Key Management (CryptoEngine Implementations)

### JPA-based (Recommended for most applications)

```java
CryptoEngine cryptoEngine = new JpaCryptoEngine(entityManagerFactory);
```

* Stores keys in a relational database via JPA
* Thread-safe and multi-node safe
* Automatically creates key entity table
* Requires `EntityManagerFactory` with `RESOURCE_LOCAL` transaction type

### JDBC-based (High performance)

```java
CryptoEngine cryptoEngine = new JdbcCryptoEngine(dataSource);
```

* Direct JDBC access to relational database
* Thread-safe and multi-node safe
* Lower overhead than JPA
* Requires `DataSource`

### HashiCorp Vault (Enterprise-grade)

```java
CryptoEngine cryptoEngine = new VaultCryptoEngine(okHttpClient, vaultUrl, token, mountPath);
```

* Enterprise-grade key security
* Keys encrypted at rest with master key
* Supports HSM, AWS KMS, Google KMS for master key
* Thread-safe and multi-node safe
* **Important:** Token must be periodically refreshed by your application

**Required Vault Policy:**
```hcl
path "secret/data/keys/*" {
  capabilities = ["create", "read", "delete"]
  # NOTE: "update" must NOT be allowed to prevent key overwrites
}
```

### In-Memory (Testing only)

```java
CryptoEngine cryptoEngine = new InMemoryCryptoEngine();
```

* Keeps keys in a local `HashMap`
* Thread-safe (single JVM only)
* **NOT for production use**

### Hardware Security Module (HSM) via PKCS#11

```java
CryptoEngine cryptoEngine = new PKCS11CryptoEngine(pkcs11ConfigPath, password);
```

* Encryption performed on HSM device
* Keys never leave the HSM
* **Not safe for concurrent access across multiple nodes**

## Encryption Details

### Algorithm

* **AES-256** (Advanced Encryption Standard) with 256-bit keys (default)
* Also supports AES-192 and AES-128
* Industry standard, used for top-secret military data
* Post-quantum secure (unlike RSA and ECC)

### Mode

* **CBC** (Cipher Block Chaining) with PKCS#5 padding
* 4-byte random pre-IV expanded to 16-byte IV via MD5
* Provides semantic security (same plaintext → different ciphertext)

### Format

Encrypted data is stored in Protocol Buffers format containing:
* Encrypted field data
* Digest for detecting encryption
* Encrypted digest for key validation
* Optional partial cleartext value (for custom replacement logic)
* Version field (future-proofing)

## Migration from 4.x to 5.x

**Breaking Changes:**

1. **Package names:** `io.axoniq.dataprotection` → `io.axoniq.framework.extension.dataprotection`
2. **API change:** `FieldEncryptingSerializer` → `FieldEncryptingConverter` (Axon 5.x Converter API)
3. **Java version:** Java 11+ → Java 21+

Update your imports and configuration accordingly.

## Performance Considerations

* **Minimal overhead:** ~1-2ms per event with AES-256
* **Key caching:** Keys cached in memory after first retrieval
* **Shaded dependencies:** Guava, Protobuf, Gson relocated to avoid conflicts
* **Idempotent operations:** Re-encrypting encrypted data is a no-op

## Receiving help

Are you having trouble using the extension?
We'd like to help you out the best we can!
There are a couple of things to consider when you're traversing anything Axon:

* Checking the [documentation](https://docs.axoniq.io/home/) should be your first stop,
  as the majority of possible scenarios you might encounter when using Axon should be covered there.
* If the Reference Guide does not cover a specific topic you would've expected,
  we'd appreciate if you could post
  a [new thread/topic on our forums describing the problem](https://discuss.axoniq.io/).
* There is a [forum](https://discuss.axoniq.io/) to support you in the case the reference guide did not sufficiently
  answer your question.
  AxonIQ's developers will help out on a best effort basis.
  Know that any support from contributors on posted question is very much appreciated on the forum.
* Next to the forum we also monitor Stack Overflow for any questions which are tagged with `axon`.

For technical questions, bug reports, and improvement requests regarding this extension, please contact support@axoniq.io.

## Licensing

Axon Framework consists out of a number of different modules, each with different licenses. Modules residing under
the [Axon Framework](https://github.com/AxonFramework) GitHub organization, with group identifier `org.axonframework`,
are Apache 2 licensed. Modules under the [AxonIQ](https://github.com/AxonIQ) GitHub organization, with group identifier
`io.axoniq`, are licensed under AxonIQ's proprietary license.

**This Data Protection Extension is commercial software licensed under AxonIQ's proprietary license.**

Please refer to individual module's LICENSE file for details.

## Resources

* **Documentation:** https://docs.axoniq.io/home/
* **Forums:** https://discuss.axoniq.io/
* **Stack Overflow:** Tag `axon`
* **Sales:** sales@axoniq.io
* **Support:** support@axoniq.io
* **Main Website:** https://axoniq.io/
