# Axon Server command wire goldens

The `.bin` files in this directory are `Command` and `CommandResponse` protobuf messages (`toByteArray()`) as
produced by the real code of released Axon Server connectors. `CommandConverterWireCompatibilityTest` pins the
current `CommandConverter` to them, so that nodes of different versions in one cluster keep understanding each
other's commands and command results.

## Versions

| Golden prefix   | `io.axoniq.framework` artifacts                                | `org.axonframework` artifacts |
|-----------------|-----------------------------------------------------------------|-------------------------------|
| `axoniq-5.3.3-` | `axon-server-connector`, `axoniq-distributed-messaging` 5.3.3   | 5.3.3                         |
| `axoniq-5.4.0-` | `axon-server-connector`, `axoniq-distributed-messaging` 5.4.0   | 5.4.0                         |

All artifacts were resolved from Maven Central into a separate local repository. The connector jars match the
SHA-1 published on Central: `axon-server-connector-5.3.3.jar` `df8fd414dea335ff5f6e7bd393d86553f2c5ed88`,
`axon-server-connector-5.4.0.jar` `b610d1a73736fbe0266886287b649233cb25104f`.

## Files

| File                                    | Produced by                                                                         |
|-----------------------------------------|-------------------------------------------------------------------------------------|
| `axoniq-<v>-command.bin`                | `CommandConverter.convertCommandMessage` for the `ValueCommand` dispatch            |
| `axoniq-<v>-void-result.bin`            | an annotated `@CommandHandler` method returning `void`                              |
| `axoniq-<v>-value-result.bin`           | a `CommandHandler` returning a `ValueResult` typed `#2.0` with metadata             |
| `axoniq-<v>-empty-string-result.bin`    | an annotated `@CommandHandler` method returning `""`                                |
| `axoniq-<v>-empty-bytes-result.bin`     | an annotated `@CommandHandler` method returning `new byte[0]`                       |
| `axoniq-<v>-error-result.bin`           | an annotated `@CommandHandler` method throwing `IllegalStateException`              |

Message identifiers of the responses are random. Request identifiers are `<sample>-command-id`; the command golden
was dispatched with identifier `value-result-command-id`, routing key `routing-key`, priority `3` and metadata
`command-key=command-value`, by client `golden-client` of component `golden-component`.

## How they were produced

`WireGoldenGenerator` (below) wires the real handling stack of a released version, the way the Axon Server
configuration enhancer sets it up: the handlers on a `SimpleCommandBus`, inside a `DistributedCommandBus` whose
connector is a `PayloadConvertingCommandBusConnector` (to `byte[]`, with the default
`DelegatingMessageConverter(DelegatingGeneralConverter(JacksonConverter))`) around the
`AxonServerCommandBusConnector`. Only the Axon Server `CommandChannel` is stubbed: it captures the command handler
the connector registers. Each command is built with the released `CommandConverter.convertCommandMessage`, fed to
that handler, and the `CommandResponse` it completes with is written to disk.

```
mvn -f <generator dir>/pom.xml -Dmaven.repo.local=<scratch repository> -Dconnector.version=5.3.3 \
    compile exec:java -Dexec.args="<output dir>"
mvn -f <generator dir>/pom.xml -Dmaven.repo.local=<scratch repository> -Dconnector.version=5.4.0 \
    compile exec:java -Dexec.args="<output dir>"
```

The generator writes `axoniq-<v>-<sample>-command.bin` for every sample; only the `value-result` one is kept here,
as `axoniq-<v>-command.bin`. `DispatchProbe` (below) feeds the goldens to the real dispatching stack of a released
version (`ConvertingCommandGateway`, `DefaultCommandGateway`, `DistributedCommandBus`,
`PayloadConvertingCommandBusConnector`, `AxonServerCommandBusConnector`, with the stub channel answering every
dispatched command with the golden) and prints what `resultAs(...)` and `getResultMessage()` return:

```
mvn -f <generator dir>/pom.xml -Dmaven.repo.local=<scratch repository> -Dconnector.version=5.3.3 \
    -Dexec.main=DispatchProbe compile exec:java -Dexec.args="<output dir>"
```

## What the goldens show

* Released 5.3.3 and 5.4.0 handlers send identical responses. A handler that returns nothing yields the command
  bus's `GenericCommandResultMessage(new MessageType(Void.class), null)`, which the connector sends as a payload
  typed `java.lang.Void`, revision `0.0.1`, without data. Neither version omits the payload.
* Released 5.3.3 and 5.4.0 dispatchers only treat a payload without type as "no result". For the `java.lang.Void`
  payload they return a result message with an empty `byte[]` payload, and `resultAs(Void.class)` fails with a
  `ConversionException` caused by Jackson's "No content to map due to end-of-input".
* Empty `String` and `byte[]` results travel as payloads of their own type without data, and are read back as `""`
  and an empty `byte[]`.

## Generator sources

`pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.axoniq.framework.wire</groupId>
    <artifactId>command-wire-golden-generator</artifactId>
    <version>1</version>

    <properties>
        <!-- Released io.axoniq.framework version whose real code produces the goldens, e.g. 5.3.3 or 5.4.0 -->
        <connector.version>5.4.0</connector.version>
        <maven.compiler.release>21</maven.compiler.release>
        <exec.main>WireGoldenGenerator</exec.main>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.axoniq.framework</groupId>
            <artifactId>axon-server-connector</artifactId>
            <version>${connector.version}</version>
        </dependency>
        <dependency>
            <groupId>io.axoniq.framework</groupId>
            <artifactId>axoniq-distributed-messaging</artifactId>
            <version>${connector.version}</version>
        </dependency>
        <!-- Present in applications; the error path of the dispatcher ExceptionConverter references it (and modelling) -->
        <dependency>
            <groupId>org.axonframework</groupId>
            <artifactId>axon-eventsourcing</artifactId>
            <version>${connector.version}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.5.0</version>
                <configuration>
                    <mainClass>${exec.main}</mainClass>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

`src/main/java/WireGoldenGenerator.java`

```java
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.command.AxonServerCommandBusConnector;
import io.axoniq.framework.axonserver.connector.command.CommandConverter;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBus;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration;
import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import org.axonframework.conversion.DelegatingGeneralConverter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.SimpleCommandBus;
import org.axonframework.messaging.commandhandling.annotation.AnnotatedCommandHandlingComponent;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.annotation.ClasspathHandlerDefinition;
import org.axonframework.messaging.core.annotation.ClasspathParameterResolverFactory;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Produces golden {@link Command} and {@link CommandResponse} wire samples with the real handling path of a released
 * Axon Server connector: an annotated command handler on a {@link SimpleCommandBus}, wrapped in a
 * {@link DistributedCommandBus} whose connector is the {@link PayloadConvertingCommandBusConnector} (to byte[], as the
 * Axon Server configuration enhancer sets it up) around the {@link AxonServerCommandBusConnector}. Only the Axon Server
 * command channel is stubbed: it captures the command handler the connector registers, so commands built by the
 * released {@link CommandConverter} can be fed to it and the resulting responses written to disk.
 * <p>
 * Usage: {@code mvn -Dconnector.version=<version> compile exec:java -Dexec.args="<output dir>"}.
 */
public class WireGoldenGenerator {

    public record VoidCommand() {
    }

    public record ValueCommand() {
    }

    public record EmptyStringCommand() {
    }

    public record EmptyBytesCommand() {
    }

    public record ErrorCommand() {
    }

    public record ValueResult(String value) {
    }

    public static class Handlers {

        @CommandHandler
        public void handle(VoidCommand command) {
        }

        @CommandHandler
        public String handle(EmptyStringCommand command) {
            return "";
        }

        @CommandHandler
        public byte[] handle(EmptyBytesCommand command) {
            return new byte[0];
        }

        @CommandHandler
        public void handle(ErrorCommand command) {
            throw new IllegalStateException("Handler failed on purpose");
        }
    }

    public static void main(String[] args) throws Exception {
        Path outputDir = Path.of(args[0]);
        Files.createDirectories(outputDir);
        String version = AxonServerCommandBusConnector.class.getPackage().getImplementationVersion();
        if (version == null) {
            version = System.getProperty("connector.version");
        }

        Map<String, Function<Command, CompletableFuture<CommandResponse>>> handlers = new ConcurrentHashMap<>();
        CommandChannel commandChannel = new CommandChannel() {
            @Override
            public Registration registerCommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler,
                                                       int loadFactor,
                                                       String... commandNames) {
                for (String commandName : commandNames) {
                    handlers.put(commandName, handler);
                }
                return new Registration() {
                    @Override
                    public CompletableFuture<Void> cancel() {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public Registration onAck(Runnable runnable) {
                        runnable.run();
                        return this;
                    }

                    @Override
                    public Registration onAck(Consumer<Optional<Throwable>> consumer) {
                        consumer.accept(Optional.empty());
                        return this;
                    }
                };
            }

            @Override
            public CompletableFuture<CommandResponse> sendCommand(Command command) {
                throw new UnsupportedOperationException("The generator only handles commands");
            }

            @Override
            public CompletableFuture<Void> prepareDisconnect() {
                return CompletableFuture.completedFuture(null);
            }
        };
        AxonServerConnection connection = (AxonServerConnection) Proxy.newProxyInstance(
                WireGoldenGenerator.class.getClassLoader(),
                new Class<?>[]{AxonServerConnection.class},
                (proxy, method, methodArgs) -> switch (method.getName()) {
                    case "commandChannel" -> commandChannel;
                    case "isConnected", "isReady" -> true;
                    case "toString" -> "StubAxonServerConnection";
                    default -> throw new UnsupportedOperationException(method.getName());
                }
        );

        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setClientId("golden-client");
        configuration.setComponentName("golden-component");
        // Mirrors the default MessageConverter of the messaging configuration defaults
        DelegatingMessageConverter messageConverter =
                new DelegatingMessageConverter(new DelegatingGeneralConverter(new JacksonConverter()));

        AxonServerCommandBusConnector axonServerConnector =
                new AxonServerCommandBusConnector(connection, configuration, messageConverter);
        axonServerConnector.start();
        PayloadConvertingCommandBusConnector connector =
                new PayloadConvertingCommandBusConnector(axonServerConnector, messageConverter, byte[].class);
        SimpleCommandBus localSegment =
                new SimpleCommandBus(new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE));
        DistributedCommandBus commandBus =
                new DistributedCommandBus(localSegment, connector, DistributedCommandBusConfiguration.DEFAULT);
        commandBus.subscribe(new AnnotatedCommandHandlingComponent<>(
                new Handlers(),
                ClasspathParameterResolverFactory.forClass(Handlers.class),
                ClasspathHandlerDefinition.forClass(Handlers.class),
                new ClassBasedMessageTypeResolver(),
                messageConverter
        ));
        // A programmatic handler, as returning a result message from an annotated handler does not keep its metadata
        commandBus.subscribe(new QualifiedName(ValueCommand.class), (command, context) -> MessageStream.just(
                new GenericCommandResultMessage(new MessageType(ValueResult.class.getName(), "2.0"),
                                                new ValueResult("some-value"),
                                                Map.of("result-key", "result-value"))
        ));

        generate(outputDir, version, "void-result", VoidCommand.class, handlers);
        generate(outputDir, version, "value-result", ValueCommand.class, handlers);
        generate(outputDir, version, "empty-string-result", EmptyStringCommand.class, handlers);
        generate(outputDir, version, "empty-bytes-result", EmptyBytesCommand.class, handlers);
        generate(outputDir, version, "error-result", ErrorCommand.class, handlers);
        System.exit(0);
    }

    private static void generate(Path outputDir,
                                 String version,
                                 String sample,
                                 Class<?> commandType,
                                 Map<String, Function<Command, CompletableFuture<CommandResponse>>> handlers)
            throws Exception {
        GenericCommandMessage commandMessage = new GenericCommandMessage(
                new GenericMessage(sample + "-command-id",
                                   new MessageType(commandType),
                                   "{}".getBytes(StandardCharsets.UTF_8),
                                   Map.of("command-key", "command-value")),
                "routing-key",
                3
        );
        Command command = CommandConverter.convertCommandMessage(commandMessage, "golden-client", "golden-component");
        Function<Command, CompletableFuture<CommandResponse>> handler = handlers.get(command.getName());
        if (handler == null) {
            throw new IllegalStateException("No handler registered for " + command.getName() + " in " + handlers);
        }
        CommandResponse response = handler.apply(command).get(10, TimeUnit.SECONDS);

        String prefix = "axoniq-" + version + "-" + sample;
        Files.write(outputDir.resolve(prefix + "-command.bin"), command.toByteArray());
        Files.write(outputDir.resolve(prefix + ".bin"), response.toByteArray());
        System.out.println("=== " + prefix + " response ===");
        System.out.println(response);
    }
}
```

`src/main/java/DispatchProbe.java`

```java
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.command.AxonServerCommandBusConnector;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBus;
import io.axoniq.framework.messaging.commandhandling.distributed.DistributedCommandBusConfiguration;
import io.axoniq.framework.messaging.commandhandling.distributed.PayloadConvertingCommandBusConnector;
import org.axonframework.conversion.DelegatingGeneralConverter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.commandhandling.CommandPriorityCalculator;
import org.axonframework.messaging.commandhandling.SimpleCommandBus;
import org.axonframework.messaging.commandhandling.annotation.AnnotationRoutingStrategy;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.commandhandling.gateway.ConvertingCommandGateway;
import org.axonframework.messaging.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Feeds golden {@link CommandResponse CommandResponses} to the real dispatching path of a released Axon Server
 * connector: a {@link ConvertingCommandGateway} around a {@link DefaultCommandGateway} on a
 * {@link DistributedCommandBus} with the {@link PayloadConvertingCommandBusConnector} and
 * {@link AxonServerCommandBusConnector}. The stubbed Axon Server command channel answers every dispatched command with
 * the response under test. Prints what {@code resultAs(...)} and {@code getResultMessage()} return per response.
 * <p>
 * Usage: {@code mvn -Dconnector.version=<version> compile exec:java -Dexec.main=DispatchProbe
 * -Dexec.args="<goldens dir>"}.
 */
public class DispatchProbe {

    public static void main(String[] args) throws Exception {
        Path goldens = Path.of(args[0]);
        String version = System.getProperty("connector.version", "?");
        AtomicReference<CommandResponse> nextResponse = new AtomicReference<>();
        CommandChannel commandChannel = new CommandChannel() {
            @Override
            public Registration registerCommandHandler(Function<Command, CompletableFuture<CommandResponse>> handler,
                                                       int loadFactor,
                                                       String... commandNames) {
                throw new UnsupportedOperationException("The probe only dispatches commands");
            }

            @Override
            public CompletableFuture<CommandResponse> sendCommand(Command command) {
                return CompletableFuture.completedFuture(
                        nextResponse.get().toBuilder().setRequestIdentifier(command.getMessageIdentifier()).build()
                );
            }

            @Override
            public CompletableFuture<Void> prepareDisconnect() {
                return CompletableFuture.completedFuture(null);
            }
        };
        AxonServerConnection connection = (AxonServerConnection) Proxy.newProxyInstance(
                DispatchProbe.class.getClassLoader(),
                new Class<?>[]{AxonServerConnection.class},
                (proxy, method, methodArgs) -> switch (method.getName()) {
                    case "commandChannel" -> commandChannel;
                    case "isConnected", "isReady" -> true;
                    case "toString" -> "StubAxonServerConnection";
                    default -> throw new UnsupportedOperationException(method.getName());
                }
        );
        DelegatingMessageConverter messageConverter =
                new DelegatingMessageConverter(new DelegatingGeneralConverter(new JacksonConverter()));
        AxonServerCommandBusConnector axonServerConnector =
                new AxonServerCommandBusConnector(connection, new AxonServerConfiguration(), messageConverter);
        axonServerConnector.start();
        DistributedCommandBus commandBus = new DistributedCommandBus(
                new SimpleCommandBus(new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)),
                new PayloadConvertingCommandBusConnector(axonServerConnector, messageConverter, byte[].class),
                DistributedCommandBusConfiguration.DEFAULT
        );
        CommandGateway gateway = new ConvertingCommandGateway(
                new DefaultCommandGateway(commandBus,
                                          new ClassBasedMessageTypeResolver(),
                                          CommandPriorityCalculator.defaultCalculator(),
                                          new AnnotationRoutingStrategy()),
                messageConverter
        );

        Map<String, CommandResponse> responses = new TreeMap<>();
        try (var files = Files.list(goldens)) {
            for (Path file : files.filter(f -> f.toString().endsWith("-result.bin")).toList()) {
                responses.put(file.getFileName().toString(), CommandResponse.parseFrom(Files.readAllBytes(file)));
            }
        }
        // The handling side of the fix omits the payload of a void result; derive that shape from the 5.4.0 golden
        CommandResponse released540Void = responses.get("axoniq-5.4.0-void-result.bin");
        responses.put("fixed-void-result (5.4.0 void golden without payload)",
                      released540Void.toBuilder().clearPayload().build());

        System.out.println("##### Dispatcher " + version);
        for (Map.Entry<String, CommandResponse> entry : responses.entrySet()) {
            nextResponse.set(entry.getValue());
            Class<?> expected = entry.getKey().contains("value") ? WireGoldenGenerator.ValueResult.class
                    : entry.getKey().contains("empty-string") ? String.class
                    : entry.getKey().contains("empty-bytes") ? byte[].class
                    : Void.class;
            System.out.println(entry.getKey()
                                       + "\n    resultAs(" + expected.getSimpleName() + ") -> "
                                       + outcome(() -> gateway.send(new WireGoldenGenerator.VoidCommand())
                                                              .resultAs(expected)
                                                              .get(10, TimeUnit.SECONDS))
                                       + "\n    getResultMessage() -> "
                                       + outcome(() -> describe(gateway.send(new WireGoldenGenerator.VoidCommand())
                                                                       .getResultMessage()
                                                                       .get(10, TimeUnit.SECONDS))));
        }
        System.exit(0);
    }

    private static String describe(Message message) {
        if (message == null) {
            return "null message";
        }
        Object payload = message.payload();
        return "type=" + message.type() + ", payload=" + render(payload) + ", metadata=" + message.metadata();
    }

    private static String render(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[] bytes) {
            return "byte[" + bytes.length + "]" + Arrays.toString(bytes);
        }
        return value.getClass().getSimpleName() + "[" + value + "]";
    }

    private static String outcome(ThrowingSupplier supplier) {
        try {
            Object value = supplier.get();
            return value instanceof String s ? "\"" + s + "\"" : render(value);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return "FAILS " + cause.getClass().getName() + ": " + cause.getMessage()
                    + (cause.getCause() != null ? " (cause " + cause.getCause() + ")" : "");
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier {

        Object get() throws Exception;
    }
}
```
