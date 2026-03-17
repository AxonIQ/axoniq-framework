/*
 * Copyright (c) 2022-2026. AxonIQ B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.platform.framework.client

import eu.rekawek.toxiproxy.ToxiproxyClient
import eu.rekawek.toxiproxy.model.ToxicDirection
import io.axoniq.platform.framework.AxoniqPlatformConfiguration
import io.axoniq.platform.framework.api.ClientSettingsV2
import io.axoniq.platform.framework.api.CommandBusInformation
import io.axoniq.platform.framework.api.EventStoreInformation
import io.axoniq.platform.framework.api.ModuleVersion
import io.axoniq.platform.framework.api.QueryBusInformation
import io.axoniq.platform.framework.api.SetupPayload
import io.axoniq.platform.framework.api.Versions
import io.axoniq.platform.framework.client.strategy.CborEncodingStrategy
import io.mockk.every
import io.mockk.mockk
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.toxiproxy.ToxiproxyContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.TimeUnit

@Testcontainers
class AxoniqConsoleRSocketClientToxiproxyIntegrationTest {

    companion object {
        // Port inside the Toxiproxy container that will be forwarded to the mock server.
        private const val PROXY_PORT = 8666

        @Container
        @JvmField
        val toxiproxy: ToxiproxyContainer = ToxiproxyContainer(
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0")
        )
    }

    private lateinit var mockServer: MockConsoleServer
    private lateinit var proxy: eu.rekawek.toxiproxy.Proxy
    private lateinit var client: AxoniqConsoleRSocketClient

    @BeforeEach
    fun setUp() {
        mockServer = MockConsoleServer()
        mockServer.start()

        // Resolve the Docker host IP as seen from inside the Toxiproxy container (the gateway
        // of the container's default network). This is more reliable than host.testcontainers.internal
        // because that hostname is only added to containers that have exposeHostPorts called
        // before their start.
        val dockerHostIp = toxiproxy.containerInfo.networkSettings.networks.values.first().gateway

        val toxiproxyClient = ToxiproxyClient(toxiproxy.host, toxiproxy.controlPort)
        proxy = toxiproxyClient.createProxy(
            "mock-server",
            "0.0.0.0:$PROXY_PORT",
            "$dockerHostIp:${mockServer.port}",
        )
    }

    @AfterEach
    fun tearDown() {
        client.disposeClient()
        proxy.delete()
        mockServer.stop()
    }

    /**
     * Cuts all traffic through the proxy (zero-bandwidth on both directions) and verifies that
     * the client detects the disconnect and reconnects once traffic is restored.
     *
     * This differs from [AxoniqConsoleRSocketClientIntegrationTest]'s reconnect test in that the
     * disconnect originates at the network layer, not the mock server's RSocket layer.
     */
    @Test
    fun `reconnects after network-level connection cut`() {
        client = buildClient()
        client.start()

        await().atMost(5, TimeUnit.SECONDS).until { client.isConnected() }

        proxy.toxics().bandwidth("CUT_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0)
        proxy.toxics().bandwidth("CUT_UPSTREAM", ToxicDirection.UPSTREAM, 0)

        await().atMost(5, TimeUnit.SECONDS).until { !client.isConnected() }

        proxy.toxics().get("CUT_DOWNSTREAM").remove()
        proxy.toxics().get("CUT_UPSTREAM").remove()

        await().atMost(20, TimeUnit.SECONDS).until { client.isConnected() }
    }

    /**
     * Simulates a network black-hole: the TCP connection stays established but all downstream
     * traffic (server → client) is delayed far beyond the heartbeat timeout. The client must
     * detect the missing heartbeats via its own timeout checker and reconnect — it cannot rely
     * on a TCP-level signal because the connection appears open.
     *
     * This is the scenario the heartbeat mechanism exists to handle.
     */
    @Test
    fun `reconnects when network becomes a black hole and heartbeats stop arriving`() {
        mockServer.clientSettings = ClientSettingsV2(
            heartbeatInterval = 200,
            heartbeatTimeout = 1000,
            processorReportInterval = 5000,
            handlerReportInterval = 5000,
            applicationReportInterval = 5000,
        )
        client = buildClient()
        client.start()

        await().atMost(5, TimeUnit.SECONDS).until { client.isConnected() }

        // Delay all server→client traffic by 60 s — the TCP connection stays "open" from both
        // sides, but the client never receives another heartbeat from the server.
        proxy.toxics().latency("black-hole", ToxicDirection.DOWNSTREAM, 60_000)

        // heartbeatTimeout=1000 ms, checker runs every 1000 ms → disconnect within ~2 s; allow 5.
        await().atMost(5, TimeUnit.SECONDS).until { !client.isConnected() }

        // Remove the toxic so the client can reconnect through the same proxy.
        proxy.toxics().get("black-hole").remove()

        await().atMost(10, TimeUnit.SECONDS).until { client.isConnected() }
    }

    /**
     * Same as [reconnects after network-level connection cut] but holds the outage for long enough
     * that the client cycles through multiple reconnect attempts.
     *
     * Unlike the immediate test (bandwidth=0 stalls SETUP frames, so retries never produce errors),
     * this test uses [eu.rekawek.toxiproxy.Proxy.disable] which causes outright "Connection
     * refused" on every attempt. That drives the retry counter up and triggers the
     * "Lost connection, will keep trying" warning log (at retryCount == 2).
     */
    @Test
    fun `recovers after extended network-level outage`() {
        client = buildClient()
        client.start()

        await().atMost(5, TimeUnit.SECONDS).until { client.isConnected() }

        // Disabling the proxy drops the existing TCP connection immediately and causes
        // subsequent reconnect attempts to fail with "Connection refused".
        proxy.disable()

        await().atMost(5, TimeUnit.SECONDS).until { !client.isConnected() }

        // Hold the outage through several retry cycles so warning logs are emitted.
        Thread.sleep(10_000)

        proxy.enable()

        await().atMost(20, TimeUnit.SECONDS).until { client.isConnected() }
    }

    /**
     * Verifies extended recovery when the client uses aggressive heartbeat settings. The short
     * [reconnects when network becomes a black hole and heartbeats stop arriving] test owns the
     * heartbeat-detection scenario; this test focuses on whether the client recovers after a long
     * outage when it was configured with a tight heartbeat timeout.
     *
     * With heartbeatTimeout=1000 ms, the client detects the proxy going down almost immediately.
     * The proxy is then held down for 10 s to observe multiple reconnect attempts and the
     * "Lost connection, will keep trying" warning log before re-enabling and verifying recovery.
     */
    @Test
    fun `recovers after extended black-hole outage`() {
        mockServer.clientSettings = ClientSettingsV2(
            heartbeatInterval = 200,
            heartbeatTimeout = 1000,
            processorReportInterval = 5000,
            handlerReportInterval = 5000,
            applicationReportInterval = 5000,
        )
        client = buildClient()
        client.start()

        await().atMost(5, TimeUnit.SECONDS).until { client.isConnected() }

        proxy.disable()

        await().atMost(3, TimeUnit.SECONDS).until { !client.isConnected() }

        Thread.sleep(10_000)

        proxy.enable()

        await().atMost(20, TimeUnit.SECONDS).until { client.isConnected() }
    }

    private fun buildClient(): AxoniqConsoleRSocketClient {
        val encodingStrategy = CborEncodingStrategy()
        val setupPayloadCreator = mockk<SetupPayloadCreator>()
        every { setupPayloadCreator.createReport() } returns minimalSetupPayload()

        val config = AxoniqPlatformConfiguration("test-env", "test-token", "test-app")
            .host(toxiproxy.host)
            .port(toxiproxy.getMappedPort(PROXY_PORT))
            .secure(false)

        return AxoniqConsoleRSocketClient(
            properties = config,
            setupPayloadCreator = setupPayloadCreator,
            registrar = RSocketHandlerRegistrar(encodingStrategy),
            encodingStrategy = encodingStrategy,
            clientSettingsService = ClientSettingsService(),
        )
    }

    private fun minimalSetupPayload() = SetupPayload(
        commandBus = CommandBusInformation(
            type = "test", axonServer = false, localSegmentType = null,
            context = null, messageSerializer = null,
        ),
        queryBus = QueryBusInformation(
            type = "test", axonServer = false, localSegmentType = null,
            context = null, messageSerializer = null, serializer = null,
        ),
        eventStore = EventStoreInformation(
            type = "test", axonServer = false, context = null,
            eventSerializer = null, snapshotSerializer = null,
        ),
        processors = emptyList(),
        versions = Versions(frameworkVersion = "test", moduleVersions = emptyList<ModuleVersion>()),
        upcasters = emptyList(),
    )
}
