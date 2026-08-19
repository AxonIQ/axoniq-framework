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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.Version;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Registry for holding workflow configurations with corresponding start conditions.
 *
 * @param <W> type of the registry.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public interface WorkflowConfigurationRegistry<W extends WorkflowConfigurationRegistry<W>>
        extends DescribableComponent {

    @Nonnull
    default W register(
            @Nonnull QualifiedName qualifiedName,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        return register(EventConditions.fromQualifiedName(qualifiedName), workflowConfiguration);
    }

    @Nonnull
    W register(
            @Nonnull EventCondition eventCondition,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    );

    @Nonnull
    Set<QualifiedName> supportedEvents();

    @Nonnull
    List<PredicatedWorkflowConfiguration> getWorkflowsConfigurations(@Nonnull QualifiedName qualifiedName);

    /**
     * Returns the configurations registered for {@code qualifiedName} whose
     * {@link WorkflowConfiguration#workflowVersion()} is the highest among all registrations (semver-ordered). Multiple
     * configurations may be returned if duplicates exist at that version (run-in-parallel semantics).
     */
    @Nonnull
    default List<PredicatedWorkflowConfiguration> getHighestVersionConfigurations(
            @Nonnull QualifiedName qualifiedName) {
        var all = getWorkflowsConfigurations(qualifiedName);
        if (all.isEmpty()) {
            return List.of();
        }
        // Use the defensive Version.tryOf(...) so a single unparseable (legacy) version string skips the
        // comparison rather than aborting startup - warnAboutSameVersionDuplicates() calls this in a loop.
        Optional<Version> highest = all.stream()
                                       .flatMap(c -> Version.tryOf(c.configuration().workflowVersion()).stream())
                                       .max(Version::compareTo);
        if (highest.isEmpty()) {
            return List.of();
        }
        Version highestVersion = highest.get();
        return all.stream()
                  .filter(c -> Version.tryOf(c.configuration().workflowVersion())
                                      .map(highestVersion::equals)
                                      .orElse(false))
                  .toList();
    }

    /**
     * Returns the configuration registered for {@code qualifiedName} whose
     * {@link WorkflowConfiguration#workflowVersion()} equals {@code version} (semver-equal). If multiple are registered
     * at that version, returns the first.
     */
    @Nonnull
    default Optional<WorkflowConfiguration<?>> getByVersion(@Nonnull QualifiedName qualifiedName,
                                                            @Nonnull String version) {
        return Version.tryOf(version).flatMap(
                target -> getWorkflowsConfigurations(qualifiedName).stream()
                                                                   .map(PredicatedWorkflowConfiguration::configuration)
                                                                   .filter(c -> Version.tryOf(c.workflowVersion())
                                                                                       .map(target::equals)
                                                                                       .orElse(false))
                                                                   .findFirst()
        );
    }

    /**
     * Finds a registered configuration by {@code workflowName} + {@code version} across all qualified names. Used to
     * route an in-flight workflow's execution to the definition matching the version it was started under (as read from
     * state, which sourced it from the started event's {@code MessageType.version()}).
     *
     * @param workflowName name of the workflow (matches {@link WorkflowConfiguration#workflowName()}).
     * @param version      semver version string to match against {@link WorkflowConfiguration#workflowVersion()}.
     * @return matching configuration, or empty if none registered.
     */
    @Nonnull
    default Optional<WorkflowConfiguration<?>> findByWorkflowNameAndVersion(@Nonnull String workflowName,
                                                                            @Nonnull String version) {
        return Version.tryOf(version).map(target -> parsedVersionsFor(workflowName).get(target));
    }

    /**
     * Finds a registered configuration by stable workflow definition id.
     *
     * @param workflowDefinitionId workflow definition id to resolve
     * @return matching configuration, or empty if none registered
     */
    @Nonnull
    default Optional<WorkflowConfiguration<?>> getWorkflowConfiguration(
            @Nonnull MessageType workflowDefinitionId
    ) {
        return findByWorkflowNameAndVersion(
                workflowDefinitionId.qualifiedName().toString(),
                workflowDefinitionId.version()
        );
    }

    /**
     * Finds the "best routing match" for a given {@code workflowName} + {@code version}: the registered configuration
     * whose {@code workflowVersion()} is the <strong>highest version less than or equal to</strong> the requested
     * {@code version} (semver-ordered). Used to dispatch a workflow whose state version was bumped mid-flight via
     * {@code ctx.migrateVersion(...)} to a value that no exact sibling is registered for - e.g. v1.0.0 + v2.0.0
     * registered, state recorded "1.0.1", we want to route to the v1.0.0 definition, not jump to v2.0.0.
     * <p>
     * Returns empty when no registered version is {@code <=} the requested one (the workflow's recorded version is
     * older than anything currently registered).
     *
     * @param workflowName name of the workflow.
     * @param version      semver state version to route against.
     * @return the closest matching configuration, or empty if none qualifies.
     */
    @Nonnull
    default Optional<WorkflowConfiguration<?>> findClosestRegisteredVersion(@Nonnull String workflowName,
                                                                            @Nonnull String version) {
        var parsed = parsedVersionsFor(workflowName);
        return Version.tryOf(version).flatMap(
                target -> Version.closestNotGreaterThan(parsed.keySet(), target)
                                 .map(parsed::get)
        );
    }

    /**
     * Finds the "next higher" routing match for {@code workflowName} + {@code version}: the registered configuration
     * whose {@code workflowVersion()} is the <strong>lowest version strictly greater than</strong> the requested
     * {@code version} (semver-ordered). Used to route an in-flight workflow forward to a newer definition when the
     * developer bumped {@code @Workflow(workflowVersion=...)} past the version recorded in the workflow's state - e.g.
     * instances started at "0.0.1" before the annotation was bumped to "0.0.2" must still find a body to replay
     * against.
     * <p>
     * Returns empty when no registered version is {@code >} the requested one.
     *
     * @param workflowName name of the workflow.
     * @param version      semver state version to route against.
     * @return the closest matching higher configuration, or empty if none qualifies.
     */
    @Nonnull
    default Optional<WorkflowConfiguration<?>> findClosestHigherRegisteredVersion(@Nonnull String workflowName,
                                                                                  @Nonnull String version) {
        var parsed = parsedVersionsFor(workflowName);
        return Version.tryOf(version).flatMap(
                target -> Version.closestHigherThan(parsed.keySet(), target)
                                 .map(parsed::get)
        );
    }

    /**
     * Returns the registered configurations for {@code workflowName} keyed by their parsed {@link Version}. Unparseable
     * version strings (legacy streams) are filtered out. The keys are the source of truth used by
     * {@link Version#closestNotGreaterThan} and {@link Version#closestHigherThan} when routing.
     */
    @Nonnull
    private Map<Version, WorkflowConfiguration<?>> parsedVersionsFor(@Nonnull String workflowName) {
        return supportedEvents().stream()
                                .flatMap(qn -> getWorkflowsConfigurations(qn).stream())
                                .map(PredicatedWorkflowConfiguration::configuration)
                                .filter(c -> workflowName.equals(c.workflowName()))
                                .flatMap(c -> Version.tryOf(c.workflowVersion())
                                                     .stream()
                                                     .map(v -> Map.entry(v, c)))
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (a, b) -> a));
    }

    /**
     * Emits a single startup warning per qualified name when multiple workflow definitions are registered at the same
     * highest version. Same-version duplicates run in parallel only if their {@code workflowIdProvider}s produce
     * distinct ids; otherwise the second start is rejected as a duplicate. Intended to be called once when the engine
     * transitions to live mode.
     */
    default void warnAboutSameVersionDuplicates() {
        for (var qualifiedName : supportedEvents()) {
            var configsAtHighest = getHighestVersionConfigurations(qualifiedName);
            if (configsAtHighest.size() > 1) {
                RoutingLog.LOGGER.warn(
                        "Multiple workflow definitions registered for '{}' at the same version '{}'. "
                                + "They will run in parallel only if their workflowIdProviders produce "
                                + "distinct ids; otherwise the second start is rejected as a same-version "
                                + "duplicate. Confirm this is intentional.",
                        qualifiedName,
                        configsAtHighest.get(0).configuration().workflowVersion());
            }
        }
    }

    /**
     * Returns all workflow versions registered under {@code workflowName} in semver ascending order. Used for
     * routing-decision logs.
     */
    @Nonnull
    default List<String> registeredVersions(@Nonnull String workflowName) {
        return supportedEvents().stream()
                                .flatMap(qn -> getWorkflowsConfigurations(qn).stream())
                                .map(PredicatedWorkflowConfiguration::configuration)
                                .filter(c -> workflowName.equals(c.workflowName()))
                                .map(WorkflowConfiguration::workflowVersion)
                                .filter(v -> v != null && !v.isBlank())
                                .distinct()
                                // One consistent ordering (transitive): parseable semver versions first,
                                // ordered by Version; unparseable (legacy) strings last, ordered
                                // lexicographically. Switching strategy per pair would violate the
                                // Comparator contract and can make the sort throw at runtime.
                                .sorted(Comparator
                                                .comparingInt((String v) -> Version.tryOf(v).isPresent() ? 0 : 1)
                                                .thenComparing(v -> Version.tryOf(v).orElse(null),
                                                               Comparator.nullsLast(Comparator.naturalOrder()))
                                                .thenComparing(Comparator.naturalOrder()))
                                .toList();
    }

    /**
     * Resolves which {@link WorkflowConfiguration} should drive an in-flight workflow's body, picking the definition
     * that matches the version recorded in state. Five-pass routing:
     * <ol>
     *   <li>{@code exact-match-start-config} - start config matches state version.</li>
     *   <li>{@code exact-match-sibling} - registry has a sibling at the exact state version.</li>
     *   <li>{@code closest-sibling} - highest registered version ≤ state (covers mid-flight
     *       {@code ctx.migrateVersion()} bumps to a value not statically registered).</li>
     *   <li>{@code closest-higher-sibling} - lowest registered version &gt; state (covers the case
     *       where {@code @Workflow(workflowVersion=...)} has been bumped past the version recorded
     *       on the workflow's started event; in-flight instances still find a body to replay).</li>
     *   <li>{@code no-match-fallback} - nothing registered for this workflow name. WARN + start
     *       config; the drift safety net pauses if step names diverge.</li>
     * </ol>
     * Unparseable state versions (legacy streams) short-circuit to the start config. Every dispatch
     * decision is logged at INFO; the fallback path logs at WARN.
     */
    @Nonnull
    default WorkflowConfiguration<?> resolveDefinitionForReplay(
            @Nonnull String workflowName,
            @Nonnull String workflowId,
            @Nonnull String stateVersion,
            @Nonnull WorkflowConfiguration<?> startConfig
    ) {
        var configVersion = startConfig.workflowVersion();
        var registered = registeredVersions(workflowName);
        // Pass 1: exact-match start config.
        try {
            if (Version.of(stateVersion).equals(Version.of(configVersion))) {
                logRoutingDecision(workflowName, workflowId, stateVersion, registered, configVersion,
                                   "exact-match-start-config",
                                   "state version equals the start-time configuration's version");
                return startConfig;
            }
        } catch (IllegalArgumentException ignored) {
            logRoutingDecision(workflowName, workflowId, stateVersion, registered, configVersion,
                               "legacy-fallback",
                               "state version is not a parseable semver string (legacy event stream)");
            return startConfig;
        }
        // Pass 2: exact-match sibling registered at the same version.
        var exact = findByWorkflowNameAndVersion(workflowName, stateVersion);
        if (exact.isPresent()) {
            logRoutingDecision(workflowName, workflowId, stateVersion, registered,
                               exact.get().workflowVersion(), "exact-match-sibling",
                               "registry has a sibling definition at the same version as recorded state");
            return exact.get();
        }
        // Pass 3: closest-sibling - highest registered version <= stateVersion.
        var closest = findClosestRegisteredVersion(workflowName, stateVersion);
        if (closest.isPresent()) {
            logRoutingDecision(workflowName, workflowId, stateVersion, registered,
                               closest.get().workflowVersion(), "closest-sibling",
                               "no exact-version sibling registered; using highest registered version <= recorded state");
            return closest.get();
        }
        // Pass 4: closest-higher-sibling - lowest registered version > stateVersion. Routes in-flight
        // workflows forward when the annotation has been bumped past the version recorded on their
        // started event (and no older sibling is still registered).
        var higher = findClosestHigherRegisteredVersion(workflowName, stateVersion);
        if (higher.isPresent()) {
            logRoutingDecision(workflowName, workflowId, stateVersion, registered,
                               higher.get().workflowVersion(), "closest-higher-sibling",
                               "no registered version <= recorded state; using lowest registered version > state");
            return higher.get();
        }
        // Pass 5: nothing registered for this workflow name at all.
        RoutingLog.LOGGER.warn(
                "Workflow {} ({}) routing: state='{}' definitions={} -> target='{}' [no-match-fallback] "
                        + "(no version registered for this workflow name; drift safety net will pause if "
                        + "step names diverge)",
                workflowName, workflowId, stateVersion, registered, configVersion);
        return startConfig;
    }

    /**
     * Convenience helper that resolves the {@link WorkflowConfiguration} for an in-flight workflow's body against the
     * registry available on the given {@link ProcessingContext}. When the registry is unavailable (e.g. tests that wire
     * the execution directly without a registry component), logs a {@code [registry-missing]} routing line and falls
     * back to {@code startConfig}.
     */
    @Nonnull
    static WorkflowConfiguration<?> resolveOrFallback(
            @Nonnull ProcessingContext ctx,
            @Nonnull String workflowName,
            @Nonnull String workflowId,
            @Nonnull String stateVersion,
            @Nonnull WorkflowConfiguration<?> startConfig
    ) {
        WorkflowConfigurationRegistry<?> registry = ctx.component(WorkflowConfigurationRegistry.class);
        if (registry == null) {
            RoutingLog.LOGGER.warn(
                    "Workflow {} ({}) routing: state='{}' definitions=<registry unavailable> -> target='{}' "
                            + "[registry-missing] (cannot look up siblings; falling back to start-time definition)",
                    workflowName, workflowId, stateVersion, startConfig.workflowVersion());
            return startConfig;
        }
        return registry.resolveDefinitionForReplay(workflowName, workflowId, stateVersion, startConfig);
    }

    private static void logRoutingDecision(String workflowName,
                                           String workflowId,
                                           String stateVersion,
                                           List<String> registeredVersions,
                                           String targetVersion,
                                           String decision,
                                           String reason) {
        RoutingLog.LOGGER.info("Workflow {} ({}) routing: state='{}' definitions={} -> target='{}' [{}] ({})",
                               workflowName, workflowId, stateVersion, registeredVersions, targetVersion,
                               decision, reason);
    }

    /**
     * Holder for the SLF4J logger used by routing decisions (interfaces cannot have static loggers directly).
     */
    final class RoutingLog {

        static final Logger LOGGER = LoggerFactory.getLogger(WorkflowConfigurationRegistry.class);

        private RoutingLog() {
        }
    }


    record PredicatedWorkflowConfiguration(
            BiPredicate<EventMessage, ProcessingContext> predicate,
            WorkflowConfiguration<?> configuration
    ) {

    }
}
