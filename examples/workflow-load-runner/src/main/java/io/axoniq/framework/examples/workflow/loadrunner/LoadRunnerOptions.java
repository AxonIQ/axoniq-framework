/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

import java.nio.file.Path;
import java.time.Duration;

/**
 * The command line options of the {@link WorkflowLoadRunner}.
 *
 * @param instances      the number of workflow instances to start, {@code 0} to only start up and measure
 * @param approvalWait   how long each instance waits for its approval before it times out
 * @param completeShare  the share of the started instances, between 0 and 1, the runner approves
 * @param segments       the number of segments of the workflow processor on first start, also its thread count
 * @param index          where the engine keeps its workflow index
 * @param inMemoryEvents whether to use an in-memory event store instead of Axon Server
 * @param axonServer     the address of Axon Server
 * @param dataDirectory  the directory holding the database of the token store and the workflow index
 * @param exitWhenDone   whether to stop after the startup measurement and starting the instances
 * @param hibernate      whether waiting workflow instances hibernate: leave memory once idle for {@code idleDelay}
 * @param idleDelay      how long a waiting instance must make no progress before it hibernates
 * @author Allard Buijze
 */
public record LoadRunnerOptions(int instances,
                                Duration approvalWait,
                                double completeShare,
                                int segments,
                                Index index,
                                boolean inMemoryEvents,
                                String axonServer,
                                Path dataDirectory,
                                boolean exitWhenDone,
                                boolean hibernate,
                                Duration idleDelay) {

    /**
     * The usage text printed for {@code --help}.
     */
    public static final String USAGE = """
            Usage: java -jar workflow-load-runner.jar <instances> [options]

              <instances>             number of workflow instances to start; 0 only starts up and measures

            Options:
              --wait=<duration>       how long an instance waits for its approval event before it times
                                      out, as an ISO-8601 duration (default: PT1H)
              --complete=<share>      share of the started instances the runner approves, 0..1 (default: 0.1)
              --segments=<n>          segments (and threads) of the workflow processor on first start (default: 16)
              --index=jpa|memory|none where to keep the workflow index: in the database, in memory, or
                                      nowhere, so every claim reads the running workflows (default: jpa)
              --axon-server=<address> Axon Server address (default: localhost:8124)
              --data-dir=<dir>        directory of the token store and workflow index database
                                      (default: ./target/load-runner-data)
              --in-memory-events      use an in-memory event store instead of Axon Server; nothing survives
                                      a restart, for trying the runner without Axon Server
              --hibernate             let waiting instances hibernate: leave memory once idle, and be
                                      restored from the event store when their approval arrives
              --idle-delay=<duration> how long a waiting instance must make no progress before it
                                      hibernates, as an ISO-8601 duration (default: PT10S)
              --exit                  stop once the instances are started and wait for their approval,
                                      instead of running until Ctrl+C
              --help                  print this text
            """;

    /**
     * Parses the given command line arguments.
     *
     * @param args the command line arguments
     * @return the parsed options
     * @throws IllegalArgumentException if an argument is unknown or malformed
     */
    public static LoadRunnerOptions parse(String... args) {
        Integer instances = null;
        Duration wait = Duration.ofHours(1);
        double complete = 0.1;
        int segments = 16;
        Index index = Index.JPA;
        boolean inMemoryEvents = false;
        String axonServer = "localhost:8124";
        Path dataDirectory = Path.of("target", "load-runner-data");
        boolean exit = false;
        boolean hibernate = false;
        Duration idleDelay = Duration.ofSeconds(10);
        for (String arg : args) {
            String value = arg.contains("=") ? arg.substring(arg.indexOf('=') + 1) : "";
            switch (arg.contains("=") ? arg.substring(0, arg.indexOf('=')) : arg) {
                case "--wait" -> wait = Duration.parse(value);
                case "--complete" -> complete = Double.parseDouble(value);
                case "--segments" -> segments = Integer.parseInt(value);
                case "--index" -> index = switch (value) {
                    case "jpa" -> Index.JPA;
                    case "memory" -> Index.MEMORY;
                    case "none" -> Index.NONE;
                    default -> throw new IllegalArgumentException("--index must be jpa, memory or none, not " + value);
                };
                case "--axon-server" -> axonServer = value;
                case "--data-dir" -> dataDirectory = Path.of(value);
                case "--in-memory-events" -> inMemoryEvents = true;
                case "--exit" -> exit = true;
                case "--hibernate" -> hibernate = true;
                case "--idle-delay" -> idleDelay = Duration.parse(value);
                default -> {
                    if (arg.startsWith("--") || instances != null) {
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                    }
                    instances = Integer.parseInt(arg);
                }
            }
        }
        if (instances == null || instances < 0) {
            throw new IllegalArgumentException("The number of instances to start is required, and must be 0 or more");
        }
        if (complete < 0 || complete > 1) {
            throw new IllegalArgumentException("--complete must be between 0 and 1");
        }
        return new LoadRunnerOptions(instances, wait, complete, segments, index, inMemoryEvents,
                                     axonServer, dataDirectory, exit, hibernate, idleDelay);
    }

    /**
     * Where the workflow engine keeps its index of running workflow instances.
     */
    public enum Index {
        /**
         * In the database, through a {@code JpaWorkflowIndex}: a restart restores the running instances from it.
         */
        JPA,
        /**
         * In memory, through an {@code InMemoryWorkflowIndex}: empty after a restart, so the first claims read the
         * running workflows from the event store, like without an index.
         */
        MEMORY,
        /**
         * Nowhere, the engine's default: every claim reads the running workflows from the event store.
         */
        NONE
    }
}
