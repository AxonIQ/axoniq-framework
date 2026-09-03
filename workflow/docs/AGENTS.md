# Workflows Reference Guide Documentation Instructions

## Important Guidelines

### Documentation Approach
- **Be user-centric**: Focus on "how do I accomplish X" rather than "how does X work internally"
- Explain what users need to do to get things done
- Avoid going too deep into implementation details unless necessary for understanding
- Keep the focus on practical usage and configuration
- Internal workings should only be explained when they affect user decisions
- Examples:
  - ✅ "To configure an event processor, use the EventProcessorModule..."
  - ❌ "The event processor internally uses a token store which maintains..."
  - ✅ "When choosing between X and Y, consider..." (when implementation matters for choice)

### Verification
**CRITICAL - Always verify before writing code:**
- Do NOT make assumptions about API details - **ALWAYS check the actual code**
- API methods may have changed in ways not fully documented
- **Before writing ANY code example:**
  1. Search for the actual class/interface in the code
  2. Read the method signatures
  3. Verify return types and parameters
  4. Check for any related classes
- API changes documentation may not cover everything
- When in doubt, ask for clarification or verification
- **Better to ask than to document incorrect APIs**

## Writing Style

### Style Guide Configuration
The documentation uses Vale for style checking. **Errors will prevent the documentation site from building.**

### Critical Rules (ERROR level - will break build)

#### 1. Acronym Capitalization
Always capitalize acronyms correctly:
- ✅ API, APIs, HTTP, HTTPS, JPA, JSON, JVM, gRPC, AMQP, CDI, BOM, DSL, OSGi, SSH, SSL, TCP, URI, URL, YAML, YML
- ❌ api, http, jpa, json, jvm, grpc, amqp
- Note: "api" is allowed as a suffix in module names (e.g., "core-api")

#### 2. Heading Capitalization
- **H1 headings**: Use title-style capitalization (Chicago style)
  - Example: "Understanding the Event Store"
- **H2-H6 headings**: Use sentence-style capitalization (only first word and proper nouns capitalized)
  - ✅ "Configuring the event store"
  - ❌ "Configuring The Event Store"
  - Exceptions: Proper product names (see list below) remain capitalized

#### 3. Proper Names
Always capitalize these names correctly:
- **Products/Frameworks**: Antora, Datadog, Dropwizard, GitHub, Gradle, JGroups, JUnit, Kafka, Kotlin, Logback, Mockito, OAuth, PostgreSQL, Testcontainers, XStream
- **Axon Products**: Axon Framework, Axon Server, AxonIQ Console, AxonIQ Cloud
- **Operating Systems**: macOS, Windows, Linux
- ❌ github, gradle, postgres, axon framework

#### 4. Axon-Specific Capitalization Exceptions
These terms are exceptions to sentence-case rules in H2-H6 headings:
- Axon Framework, Axon Server, Axon Configuration, Axon Messaging, Axon Test, AxonIQ Console
- Command-Query Responsibility Separation (CQRS)
- Domain-Driven Design (DDD)
- Event Sourcing
- Spring, Spring Boot, Spring Boot Starter
- OpenTelemetry, OpenTracing
- And other product names (see full list in Headings.yml)

### Warnings (won't break build but should avoid)
- Remove annotation comments like TODO, FIXME, XXX, NOTE before committing
- Watch for common misspellings (e.g., "poplar" instead of "popular")
- Avoid overly long sentences (Vale may flag sentences that are too complex - break them into shorter, clearer sentences)

### Em-dash prohibition
**Never use em-dashes (`—`) in documentation.** Em-dashes read as machine-generated and make the text feel less human. Use alternative punctuation instead:
- Replace `—` with a comma, colon, semicolon, or parentheses depending on the context
- ✅ "The processor starts immediately, consuming from the head of the stream."
- ✅ "The processor starts immediately (consuming from the head of the stream)."
- ❌ "The processor starts immediately — consuming from the head of the stream."

### ASCII-only text
**All `.adoc` files must contain only ASCII characters.** Never use:
- Curly/smart quotes: `"` `"` `'` `'` — use straight `"` and `'` instead
- Em-dash: `—` — use a comma, colon, or parentheses instead (see above)
- Ellipsis: `…` — use three dots `...` instead
- Any other non-ASCII Unicode character

**LF line endings only** — never use CR (`\r`) or CRLF. Configure your editor to write LF.

---

## Critical Reminders

### After Completing Any Documentation Update

**YOU MUST:**
1. ✅ Verify all xrefs point to existing files (use old filenames until files are renamed)
2. ✅ Check style guide compliance (heading capitalization, acronyms, product names)

**When renaming a file:**
1. ✅ Use `Grep` to find ALL xrefs to the old filename across entire reference guide
2. ✅ Update every xref to use the new filename

---