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

package io.axoniq.framework.testcontainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Holds the single canonical {@link AxonServerContainer} configuration meant to be shared by every IT test suite in
 * the build, instead of each test class declaring its own.
 * <p>
 * Testcontainers' reuse feature (a container-level {@link AxonServerContainer#withReuse(boolean)} combined with
 * {@code testcontainers.reuse.enable=true} in {@code ~/.testcontainers.properties}) attaches to an already-running
 * container instead of starting a new one, but only when the requested configuration hashes identically to one
 * already running. Every test class that used to hand-roll its own {@link AxonServerContainer} therefore started a
 * fresh container, even within the same build. Depending on {@link #INSTANCE} here instead, rather than
 * constructing a separate {@link AxonServerContainer}, is what lets those requests collapse onto the same running
 * container.
 * <p>
 * This class lives in this module specifically because it is low enough in the dependency graph for every consumer
 * -- {@code connector/axon-server-connector}, {@code dependency-injection/spring/spring-boot-autoconfigure}, and all
 * {@code integrationtests/*} modules -- to reach it directly.
 *
 * @author John Hendrikx
 * @since 5.4.0
 */
public final class SharedAxonServerContainer {

    /**
     * Classpath resource name of the Axon Server Enterprise test license. Only present (and thus only mounted, see
     * {@link #INSTANCE}) in modules whose test classpath includes it; absent elsewhere. Tests that don't need
     * multi-context features work identically either way.
     */
    public static final String LICENSE_RESOURCE = "axon-server-test.license";

    /**
     * The shared container instance. Byte-identical construction is the entire point: it is what lets Testcontainers'
     * reuse hash match across every module and JVM fork that references it.
     */
    public static final AxonServerContainer INSTANCE =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:latest")
                    .withAxonServerHostname("localhost")
                    // Matches the node name AxonServerContainerUtils.createContext hardcodes into its role
                    // assignment ("axonserver"); without this, the node's identity defaults to the hostname
                    // above, and any createContext call fails with "Node is not active: [axonserver]".
                    .withAxonServerName("axonserver")
                    // AxonServerTestInfrastructure (multitenancy suites) deliberately does not reuse across JVMs,
                    // so it can't collapse onto this container regardless -- this label is just defense-in-depth
                    // documentation of that separation, in case reuse there is ever turned back on.
                    .withLabel("io.axoniq.container-pool", "shared")
                    // AxonServerContainer's own constructor bakes the Surefire/Failsafe fork number into
                    // TESTCONTAINERS_FORK_NUMBER, so ad-hoc containers in different concurrent forks don't
                    // accidentally collide on the same Docker container. For this instance that's counterproductive:
                    // it would otherwise vary the reuse hash per fork, so "the shared container" ends up as one
                    // separate container per fork instead of truly one for the whole build. Override it back to a
                    // constant so the reuse hash -- and thus the underlying container -- stays the same everywhere.
                    .withEnv("TESTCONTAINERS_FORK_NUMBER", "shared")
                    .withDevMode(true)
                    .withReuse(true)
                    .withDcbContext(true)
                    .withLicense(licenseExists() ? LICENSE_RESOURCE : null);

    /**
     * Path of the cross-process lock file guarding {@link #ensureStarted()}. A fixed name is enough: there is only
     * ever one {@link #INSTANCE} to guard.
     */
    private static final Path START_LOCK_FILE =
            Paths.get(System.getProperty("java.io.tmpdir"), "shared-axon-server-container-start.lock");

    /**
     * Starts {@link #INSTANCE} if it isn't already running.
     * <p>
     * {@link AxonServerContainer#start()} is idempotent on its own -- Testcontainers makes it a no-op when the
     * container is already running in this JVM -- so this is a thin, explicitly-named entry point for callers rather
     * than a correctness requirement.
     * <p>
     * {@code synchronized} alone only guards within one JVM, not across forked processes, and Testcontainers'
     * own {@code findContainerForReuse()} has no locking of its own (confirmed via its source: a {@code // TODO
     * locking} comment). Two Surefire/Failsafe forks can therefore both decide {@code INSTANCE} isn't running yet
     * and both end up creating their own container instead of one attaching to the other's -- confirmed directly:
     * from a clean Docker state, two forks calling this method concurrently produced two separate containers on
     * two different ports, even though both compute the identical reuse hash. A {@link FileLock} serializes the
     * whole check-and-start sequence across processes, so the second caller sees the first one's container already
     * running (and matching on hash) by the time it gets to check. Kept {@code synchronized} too: a second
     * {@link FileLock} acquisition attempt from another thread in the same JVM throws
     * {@link java.nio.channels.OverlappingFileLockException} rather than waiting, so cross-thread safety within
     * this JVM still needs the intrinsic lock.
     */
    public static synchronized void ensureStarted() {
        if (INSTANCE.isRunning()) {
            return;
        }
        try (FileChannel channel = FileChannel.open(START_LOCK_FILE, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock lock = channel.lock()) {
            if (!INSTANCE.isRunning()) {
                INSTANCE.start();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Checks whether {@link #LICENSE_RESOURCE} is present on this JVM's classpath.
     *
     * @return {@code true} if the license resource is present and non-empty, {@code false} otherwise
     */
    public static boolean licenseExists() {
        try (var resource = SharedAxonServerContainer.class.getResourceAsStream("/" + LICENSE_RESOURCE)) {
            return resource != null && resource.read() != -1;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SharedAxonServerContainer() {
        // Utility class
    }
}
