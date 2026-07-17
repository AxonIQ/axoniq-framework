# Documentation Code Samples

This module compiles the Java code samples that are included in the documentation. The sample
sources live in the Antora `examples` directories of each documentation component
(`docs/<guide>/modules/<module>/examples`) and are pulled into the pages through tagged regions.
The module is part of the default Maven reactor: a framework API change that breaks a documented
sample breaks the build. It is never deployed.

The main Antora documentation build lives in a **separate project**; this repository only
contributes some of the pages and partials that the external build includes. This module therefore
exists purely to compile those samples against the freshly built Axoniq Framework artifacts.

## Scope

Only pages that document APIs present in this repository are covered. Modules that document
features not available in this codebase (for example the removed sagas and deadlines features, or
Axon Framework 4 "before" migration snippets) keep their code inline and are out of scope. The
`examples` directories currently registered as source roots are:

- `reference-guide/modules/dead-letter-queue`
- `reference-guide/modules/message-transformation`
- `reference-guide/modules/distributed-messaging`
- `reference-guide/modules/connector`
- `reference-guide/modules/snapshotting`

## How a page includes a sample

```adoc
[source,java]
----
include::example$deadletterqueue/index/CustomEnqueuePolicy.java[tag=enqueue-policy,indent=0]
----
```

In the Java file, regions are delimited with tag comments:

```java
// tag::enqueue-policy[]
...
// end::enqueue-policy[]
```

`example$` resolves against the `examples` directory of the module the including page **or
partial** belongs to, so partials include samples the same way pages do.

## Conventions

- **Package per page/partial**: `<antoramodule>.<pagename>`, lowercase with dashes removed
  (page `dead-letter-queue/pages/index.adoc` -> package `deadletterqueue.index`,
  partial `distributed-messaging/partials/distributed-command-bus.adoc` ->
  package `distributedmessaging.distributedcommandbus`).
  Use a deeper sub-package when a page displays the same class name more than once in different
  variants.
- **Self-contained pages**: every supporting type a snippet references (commands, events, queries,
  services, id types) is defined in the page's own package, so rendered snippets need no imports
  for them. Framework imports shown in the original snippet stay inside the tag; imports the
  original snippet did not show stay outside the tag.
- **Tag names describe the snippet** (`tag=configure-dead-letter-queue`), never positions.
- **Callouts** stay as `// <1>` comments inside the tagged region; Asciidoctor renders them as
  callout markers.
- **Nested types + `indent=0`**: a snippet that displays a top-level-looking record or method may
  live nested inside a wrapper class; `indent=0` strips the extra indentation. Use member imports
  (`import pkg.Outer.Inner;`) to keep references unqualified in the display.
- **Multi-fragment blocks** (for example records followed by bare handler methods) become several
  sibling top-level package-private classes in one file, one tag each, included with `tags=a;b;c`.
  Keep all tagged regions at the same nesting depth (indent normalization is per-include, not
  per-tag) and end a region with a blank line to separate it from the next.
- **Intentionally non-compiling blocks** stay inline in the page and are marked with a role:
  `[source,java,role=axon4]` for Axon Framework 4 "before" code on migration pages,
  `[source,java,role=pseudocode]` for illustrative pseudo-code (for example a snippet that elides a
  required argument with `/* ... */`). Everything else must be an include.
- **ASCII only, LF line endings**, 4-space indentation, no license headers in sample files.

## Verifying

Compile everything (from the repository root):

```bash
./mvnw -pl docs/_samples test-compile
```

Check that a converted page renders the same code as before conversion:

```bash
python3 docs/_samples/bin/compare-snippets.py docs/reference-guide/modules/dead-letter-queue/pages/index.adoc HEAD
```

Every difference the script reports must be intentional (for example a dropped import of a
sample-local type). If compilation reveals that a documented snippet was wrong, fix the sample so
it keeps its teaching intent and record the finding in the commit message.
