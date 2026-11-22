/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.examples.workflow.loadrunner;

import io.axoniq.framework.examples.workflow.loadrunner.LoadWorkflow.LoadApproved;
import io.axoniq.framework.examples.workflow.loadrunner.LoadWorkflow.LoadStarted;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowInstance;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.EventTrackerStatus;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Starts many instances of the {@link LoadWorkflow} and measures how long the application takes to start up while
 * workflow instances are in progress.
 * <p>
 * Run it with the number of instances to start as first argument, see {@link LoadRunnerOptions#USAGE}. The runner:
 * <ol>
 *     <li>starts the application and measures when the workflow processor is ready: every segment claimed, the
 *     running workflow instances of each segment restored, and caught up with the event store;</li>
 *     <li>publishes the start events of the requested number of instances in batches, and measures how fast they are
 *     published and how fast the instances start;</li>
 *     <li>approves a share of the instances: it finds the instances waiting for their approval through the
 *     {@link WorkflowManager} and publishes a {@link LoadApproved} event for each;</li>
 *     <li>logs its progress every five seconds, until stopped with Ctrl+C.</li>
 * </ol>
 * The runner counts the workflow instances in memory and the hibernated ones through the
 * {@link WorkflowEngine} of its workflow module. That is an internal component, used here because the
 * {@link WorkflowManager} has no such counts, and sources every hibernated instance from the event store to answer a
 * query; a measurement tool like this runner should not do that every few seconds.
 *
 * @author Allard Buijze
 */
public final class WorkflowLoadRunner implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowLoadRunner.class);
    private static final Logger report = LoggerFactory.getLogger("report");

    private static final int PUBLISH_BATCH_SIZE = 100;
    private static final int MAX_BATCHES_IN_FLIGHT = 16;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final LoadRunnerOptions options;
    private final String runId = "load-" + Long.toString(System.currentTimeMillis(), 36);
    private final LoadStatistics statistics = new LoadStatistics(runId);
    private final Set<String> approved = ConcurrentHashMap.newKeySet();
    private final Set<String> toApprove = ConcurrentHashMap.newKeySet();
    private final AtomicInteger peakResident = new AtomicInteger();
    private final AtomicLong rehibernatedAfterMillis = new AtomicLong(-1);
    private volatile long mainStartedAt = System.nanoTime();
    private volatile boolean processorReady;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, runnable -> {
        var thread = new Thread(runnable, "load-runner");
        thread.setDaemon(true);
        return thread;
    });

    private @Nullable EntityManagerFactory entityManagerFactory;
    private @Nullable AxonConfiguration configuration;
    private @Nullable WorkflowManager workflowManager;
    private @Nullable WorkflowEngine workflowEngine;
    private @Nullable EventGateway eventGateway;
    private @Nullable MessageTypeResolver messageTypeResolver;
    private @Nullable EventConverter eventConverter;

    /**
     * Creates a load runner with the given options. Call {@link #startUp()} to start the application.
     *
     * @param options the options of this run
     */
    public WorkflowLoadRunner(LoadRunnerOptions options) {
        this.options = options;
    }

    /**
     * Runs the load runner with the given command line arguments, see {@link LoadRunnerOptions#USAGE}.
     *
     * @param args the command line arguments
     * @throws InterruptedException when interrupted while running
     */
    public static void main(String[] args) throws InterruptedException {
        long mainStarted = System.nanoTime();
        if (Arrays.asList(args).contains("--help")) {
            report.info(LoadRunnerOptions.USAGE);
            return;
        }
        LoadRunnerOptions options;
        try {
            options = LoadRunnerOptions.parse(args);
        } catch (IllegalArgumentException e) {
            report.error("{}\n\n{}", e.getMessage(), LoadRunnerOptions.USAGE);
            System.exit(2);
            return;
        }

        var runner = new WorkflowLoadRunner(options);
        var stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runner.close();
            stopped.countDown();
        }, "load-runner-shutdown"));

        try {
            runner.startUp(mainStarted);
            runner.startInstances();
            if (options.exitWhenDone()) {
                runner.awaitInstancesWaiting();
                System.exit(0);
            }
        } catch (RuntimeException e) {
            logger.error("The load runner failed", e);
            System.exit(1);
        }
        report.info("Running until stopped with Ctrl+C.");
        stopped.await();
    }

    /**
     * Starts the application and waits until the workflow processor is ready, reporting how long that took.
     *
     * @return the startup measurement
     */
    public StartupReport startUp() {
        return startUp(System.nanoTime());
    }

    private StartupReport startUp(long startedAt) {
        mainStartedAt = startedAt;
        report.info("Starting: {} instances, {} index, hibernation {}, event store {}, data in {}",
                    options.instances(),
                    options.index(),
                    options.hibernate() ? "after " + options.idleDelay() : "disabled",
                    options.inMemoryEvents() ? "in memory" : "Axon Server at " + options.axonServer(),
                    options.inMemoryEvents() ? "memory" : options.dataDirectory().toAbsolutePath());
        long jvmToMain = System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)
                - ManagementFactory.getRuntimeMXBean().getStartTime();

        entityManagerFactory = LoadRunnerSetup.entityManagerFactory(options);
        Duration databaseOpened = since(startedAt);

        configuration = LoadRunnerSetup.configurer(options, entityManagerFactory, statistics).start();
        Duration configurationStarted = since(startedAt);
        workflowManager = configuration.getComponents(WorkflowManager.class)
                                       .get("WorkflowManager[" + LoadRunnerSetup.MODULE + "]");
        workflowEngine = configuration.getComponents(WorkflowEngine.class)
                                      .get("WorkflowEngine[" + LoadRunnerSetup.MODULE + "]");
        scheduler.scheduleWithFixedDelay(this::sampleResidency, 0, 1, TimeUnit.SECONDS);
        eventGateway = configuration.getComponent(EventGateway.class);
        messageTypeResolver = configuration.getComponent(MessageTypeResolver.class);
        eventConverter = configuration.getComponent(EventConverter.class);
        scheduler.scheduleWithFixedDelay(this::logProgress, 5, 5, TimeUnit.SECONDS);

        Duration segmentsClaimed = awaitProcessorReady(startedAt);
        Duration processorReady = since(startedAt);
        this.processorReady = true;
        int inProgress = inProgress();

        var startup = new StartupReport(Duration.ofMillis(jvmToMain), databaseOpened, configurationStarted,
                                        segmentsClaimed, processorReady, inProgress);
        report.info("""

                            === Startup ===
                            JVM start -> main:                   {} ms
                            main -> database opened:             {} ms
                            main -> configuration started:       {} ms
                            main -> all segments claimed:        {} ms
                            main -> workflow processor ready:    {} ms
                            workflow instances in progress:      {}
                            {}
                            """,
                    startup.jvmToMain().toMillis(), startup.databaseOpened().toMillis(),
                    startup.configurationStarted().toMillis(), startup.segmentsClaimed().toMillis(),
                    startup.processorReady().toMillis(), startup.inProgress(), memory());
        if (options.completeShare() > 0) {
            scheduler.scheduleWithFixedDelay(this::approveWaitingInstances, 1, 1, TimeUnit.SECONDS);
        }
        return startup;
    }

    /**
     * Publishes the start events of the requested number of instances and waits until they all started, reporting
     * the throughput.
     *
     * @return the start measurement
     * @throws InterruptedException when interrupted while waiting for the instances to start
     */
    public StartReport startInstances() throws InterruptedException {
        int instances = options.instances();
        if (instances == 0) {
            return new StartReport(0, Duration.ZERO, Duration.ZERO);
        }
        long toComplete = Math.round(instances * options.completeShare());
        report.info("Starting {} instances with ids {}-<n>, approving {} of them", instances, runId, toComplete);

        long startedAt = System.nanoTime();
        var inFlight = new Semaphore(MAX_BATCHES_IN_FLIGHT);
        var failure = new AtomicReference<Throwable>();
        for (int first = 0; first < instances && failure.get() == null; first += PUBLISH_BATCH_SIZE) {
            var batch = new ArrayList<LoadStarted>();
            for (int n = first; n < Math.min(first + PUBLISH_BATCH_SIZE, instances); n++) {
                batch.add(new LoadStarted(runId + "-" + n, n < toComplete));
                if (n < toComplete) {
                    toApprove.add(runId + "-" + n);
                }
            }
            inFlight.acquire();
            publish(batch).whenComplete((result, error) -> {
                inFlight.release();
                if (error == null) {
                    statistics.recordPublished(batch.size());
                } else {
                    failure.compareAndSet(null, error);
                }
            });
        }
        inFlight.acquire(MAX_BATCHES_IN_FLIGHT);
        inFlight.release(MAX_BATCHES_IN_FLIGHT);
        if (failure.get() != null) {
            throw new IllegalStateException("Publishing the start events failed", failure.get());
        }
        Duration published = since(startedAt);

        while (statistics.startedThisRun() < instances) {
            Thread.sleep(50);
        }
        Duration started = since(startedAt);

        var start = new StartReport(instances, published, started);
        report.info("""

                            === Starting {} instances ===
                            start events published in:           {} ms ({} events/s)
                            instances started in:                {} ms ({} instances/s)
                            workflow instances in progress:      {}
                            {}
                            """,
                    instances,
                    published.toMillis(), perSecond(instances, published),
                    started.toMillis(), perSecond(instances, started),
                    inProgress(), memory());
        return start;
    }

    /**
     * Waits until every workflow instance in progress waits for its approval, so that stopping the application does
     * not interrupt any other step.
     *
     * @throws InterruptedException when interrupted while waiting
     */
    public void awaitInstancesWaiting() throws InterruptedException {
        var waiting = WorkflowStateQuery.byStepStatus(LoadWorkflow.AWAIT_APPROVAL_STEP, StepStatus.STARTED);
        // A workflow only hibernates while it waits, so every hibernated workflow waits for its approval.
        while (inProgress() > residentMatching(waiting).size() + workflowEngine.hibernatedWorkflowIds().size()) {
            Thread.sleep(200);
        }
        report.info("All {} workflow instances in progress wait for their approval.", inProgress());
    }

    /**
     * Returns the statistics of this run.
     *
     * @return the statistics of this run
     */
    public LoadStatistics statistics() {
        return statistics;
    }

    /**
     * Returns the number of workflow instances in progress on this node: the executions in memory with status
     * {@code STARTED}, plus the hibernated workflows.
     *
     * @return the number of workflow instances in progress
     */
    public int inProgress() {
        var inProgress = new HashSet<>(residentMatching(WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED)));
        inProgress.addAll(workflowEngine.hibernatedWorkflowIds());
        return inProgress.size();
    }

    /**
     * Returns the number of hibernation hints in the JPA workflow index: the hibernated workflows a claim of their
     * segment leaves out of memory. The index only writes a hint within the transaction the processor stores its token
     * in, so this shows that the index joins that transaction.
     *
     * @return the number of entries of the JPA workflow index with a hibernation hint, or {@code 0} when the runner
     * keeps no JPA index
     */
    public long hibernationHintsInIndex() {
        if (options.index() != LoadRunnerOptions.Index.JPA || entityManagerFactory == null) {
            return 0;
        }
        return LoadRunnerSetup.hibernationHints(entityManagerFactory);
    }

    /**
     * Describes the hibernation hints in the JPA workflow index for the progress line, when there can be any.
     */
    private String hintsInIndex() {
        return options.hibernate() && options.index() == LoadRunnerOptions.Index.JPA
                ? ", hibernation hints in index " + hibernationHintsInIndex()
                : "";
    }

    /**
     * Returns the ids of the workflow executions in memory whose state matches the given query.
     */
    private Set<String> residentMatching(WorkflowStateQuery query) {
        var matching = new HashSet<String>();
        for (WorkflowExecution execution : workflowEngine.workflowExecutions()) {
            if (WorkflowStateQueryMatcher.matches(query, execution.state())) {
                matching.add(execution.workflowId());
            }
        }
        return matching;
    }

    /**
     * Describes the memory use of this node: the workflow executions in memory, the hibernated workflows, and the
     * used heap right after a {@link System#gc()}. The heap is approximate: {@code System.gc()} is only a request,
     * and the used heap includes everything else the application holds.
     */
    private String memory() {
        System.gc();
        var runtime = Runtime.getRuntime();
        long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        return "executions in memory " + workflowEngine.workflowExecutions().size()
                + ", hibernated " + workflowEngine.hibernatedWorkflowIds().size()
                + ", used heap after GC ~" + usedMb + " MB";
    }

    /**
     * Once a second, tracks the peak number of executions in memory, and, with hibernation, when at least 99% of the
     * instances in progress were hibernated after the processor was ready, measured from {@code main}.
     */
    private void sampleResidency() {
        try {
            int resident = workflowEngine.workflowExecutions().size();
            peakResident.accumulateAndGet(resident, Math::max);
            if (options.hibernate() && processorReady && rehibernatedAfterMillis.get() < 0 && peakResident.get() > 0) {
                int hibernated = workflowEngine.hibernatedWorkflowIds().size();
                if (hibernated > 0 && hibernated >= 0.99 * (resident + hibernated)) {
                    rehibernatedAfterMillis.set(since(mainStartedAt).toMillis());
                    report.info("main -> 99% of the instances in progress hibernated: {} ms (peak executions in"
                                        + " memory {})", rehibernatedAfterMillis.get(), peakResident.get());
                }
            }
        } catch (RuntimeException e) {
            logger.warn("Sampling the executions in memory failed", e);
        }
    }

    /**
     * Publishes the given payloads as events.
     * <p>
     * Attaching the {@link EventConverter} lets the workflow engine read the payloads as maps when they come from the
     * in-memory event store, which hands out the published objects as they are. Events read from Axon Server always
     * carry a converter.
     */
    private CompletableFuture<Void> publish(List<?> payloads) {
        var events = payloads.stream()
                             .map(payload -> new GenericEventMessage(messageTypeResolver.resolveOrThrow(payload),
                                                                     payload)
                                     .withConverter(eventConverter))
                             .toList();
        return eventGateway.publish(null, events);
    }

    /**
     * Waits until the workflow processor claimed all segments of its token store and is caught up with the event
     * store in each of them.
     * <p>
     * The processor restores the running workflow instances of a segment when it claims the segment, before it
     * processes any event of it. Being caught up in every segment therefore also means every running instance is
     * restored. A segment shows up in the processing status when the processor starts restoring its instances; the
     * processor restores its newly claimed segments one after the other.
     *
     * @return the time from the given start until every segment showed up in the processing status
     */
    private Duration awaitProcessorReady(long startedAt) {
        var processor = configuration.getComponents(StreamingEventProcessor.class).get(LoadRunnerSetup.MODULE);
        var tokenStore = configuration.getComponent(TokenStore.class);
        var unitOfWorkFactory = configuration.getComponent(UnitOfWorkFactory.class);
        @Nullable Duration claimed = null;
        while (true) {
            // The JpaTokenStore requires a processing context, also for reading.
            int segments = unitOfWorkFactory.create()
                                            .executeWithResult(context -> tokenStore.fetchSegments(
                                                    LoadRunnerSetup.MODULE, context
                                            ))
                                            .orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                                            .join()
                                            .size();
            Map<Integer, EventTrackerStatus> status = processor.processingStatus();
            if (claimed == null && segments > 0 && status.size() == segments) {
                claimed = since(startedAt);
            }
            if (claimed != null && status.size() == segments
                    && status.values().stream().allMatch(EventTrackerStatus::isCaughtUp)) {
                return claimed;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the workflow processor", e);
            }
        }
    }

    /**
     * Approves the instances that wait for their approval and are marked to be completed, by publishing a
     * {@link LoadApproved} event for each.
     */
    private void approveWaitingInstances() {
        if (options.hibernate()) {
            // Approves hibernated instances only, so the approval latency includes waking them.
            approve(workflowEngine.hibernatedWorkflowIds().stream()
                                  .filter(toApprove::contains)
                                  .filter(approved::add)
                                  .map(LoadApproved::new)
                                  .toList());
            return;
        }
        try {
            var waiting = workflowManager.findMany(
                    WorkflowStateQuery.byStepStatus(LoadWorkflow.AWAIT_APPROVAL_STEP, StepStatus.STARTED)
                                      .payloadValue("complete", true)
            );
            List<WorkflowState> states = collect(waiting.instances())
                    .thenCompose(WorkflowLoadRunner::states)
                    .orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                    .join();
            List<LoadApproved> approvals = states.stream()
                                                 .map(WorkflowState::workflowId)
                                                 .filter(approved::add)
                                                 .map(LoadApproved::new)
                                                 .toList();
            approve(approvals);
        } catch (RuntimeException e) {
            logger.warn("Approving the waiting instances failed", e);
        }
    }

    private void approve(List<LoadApproved> approvals) {
        try {
            for (int first = 0; first < approvals.size(); first += PUBLISH_BATCH_SIZE) {
                var batch = approvals.subList(first, Math.min(first + PUBLISH_BATCH_SIZE, approvals.size()));
                statistics.recordApprovalsPublishing(batch.stream().map(LoadApproved::loadId).toList());
                publish(batch).orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS).join();
                statistics.recordApprovals(batch.size());
            }
        } catch (RuntimeException e) {
            logger.warn("Approving the waiting instances failed", e);
        }
    }

    private void logProgress() {
        try {
            var processor = configuration.getComponents(StreamingEventProcessor.class).get(LoadRunnerSetup.MODULE);
            var status = processor.processingStatus().values();
            report.info("Progress: published {}, started {}, in progress {}, approvals {}, completed {}, failed {}"
                                + " | processor: {} segments, {} caught up | {}, peak executions in memory {}{}"
                                + " | approval -> completed: {}",
                        statistics.published(), statistics.startedThisRun(), inProgress(),
                        statistics.approvalsPublished(), statistics.completed(), statistics.failed(),
                        status.size(), status.stream().filter(EventTrackerStatus::isCaughtUp).count(),
                        memory(), peakResident.get(), hintsInIndex(), statistics.approvalLatencies());
        } catch (RuntimeException e) {
            logger.warn("Reporting the progress failed", e);
        }
    }

    /**
     * Stops the application: the workflow processor stores its tokens and releases its segments, after which the
     * database is closed.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        scheduler.shutdownNow();
        if (workflowEngine != null) {
            report.info("Stopping: {}, peak executions in memory {}, approval -> completed: {}",
                        memory(), peakResident.get(), statistics.approvalLatencies());
        } else {
            report.info("Stopping...");
        }
        if (configuration != null) {
            configuration.shutdown();
        }
        if (entityManagerFactory != null) {
            entityManagerFactory.close();
        }
        report.info("Stopped. Published {} start events and {} approvals; {} instances completed, {} failed.",
                    statistics.published(), statistics.approvalsPublished(), statistics.completed(),
                    statistics.failed());
    }

    private static CompletableFuture<List<WorkflowState>> states(List<WorkflowInstance> instances) {
        var states = instances.stream().map(WorkflowInstance::state).toList();
        return CompletableFuture.allOf(states.toArray(CompletableFuture[]::new))
                                .thenApply(ignored -> states.stream().map(CompletableFuture::join).toList());
    }

    /**
     * Collects every item of the given publisher.
     */
    private static <T> CompletableFuture<List<T>> collect(Publisher<T> publisher) {
        var result = new CompletableFuture<List<T>>();
        publisher.subscribe(new Subscriber<T>() {
            private final List<T> items = new ArrayList<>();

            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T item) {
                items.add(item);
            }

            @Override
            public void onError(Throwable error) {
                result.completeExceptionally(error);
            }

            @Override
            public void onComplete() {
                result.complete(items);
            }
        });
        return result;
    }

    private static Duration since(long startedAt) {
        return Duration.ofNanos(System.nanoTime() - startedAt);
    }

    private static long perSecond(long count, Duration duration) {
        return duration.isZero() ? 0 : Math.round(count * 1000.0 / Math.max(1, duration.toMillis()));
    }

    /**
     * How long the application took to start up.
     *
     * @param jvmToMain            from the start of the JVM until {@code main} was invoked
     * @param databaseOpened       from {@code main} until the database was open
     * @param configurationStarted from {@code main} until the configuration was started
     * @param segmentsClaimed      from {@code main} until the workflow processor claimed every segment
     * @param processorReady       from {@code main} until the workflow processor claimed every segment, restored the
     *                             running instances, and caught up with the event store
     * @param inProgress           the number of workflow instances in progress once the processor was ready
     */
    public record StartupReport(Duration jvmToMain,
                                Duration databaseOpened,
                                Duration configurationStarted,
                                Duration segmentsClaimed,
                                Duration processorReady,
                                int inProgress) {

    }

    /**
     * How fast the instances were started.
     *
     * @param instances the number of instances started
     * @param published how long publishing all start events took
     * @param started   how long it took until every instance started
     */
    public record StartReport(int instances, Duration published, Duration started) {

    }
}
