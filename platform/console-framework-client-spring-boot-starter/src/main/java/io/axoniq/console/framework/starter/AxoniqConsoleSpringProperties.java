/*
 * Copyright (c) 2022-2024. AxonIQ B.V.
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

package io.axoniq.console.framework.starter;

import io.axoniq.console.framework.AxoniqConsoleDlqMode;
import io.axoniq.console.framework.DomainEventAccessMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "axoniq.console")
public class AxoniqConsoleSpringProperties {

    /**
     * The host to connect to. Defaults to {@code framework.console.axoniq.io}.
     */
    private String host = "framework.console.axoniq.io";
    /**
     * The port to connect to. Defaults to {@code 7000}.
     */
    private Integer port = 7000;
    /**
     * The credentials used to connect to the AxonIQ Console. The module will not work without setting this.
     */
    private String credentials = null;
    /**
     * The display name of the application in the UI. Defaults to the application name of the Spring Boot application.
     * Some special characters, such as [ and ] will be filtered out of the name and replaced with a hyphen.
     */
    private String applicationName = null;
    /**
     * The mode of DLQ operations. Defaults to {@code FULL}, which can return sensitive information to the UI. If this
     * concerns you, consider {@code MASKED} to mask potentially sensitive data, or {@code NONE} to disable DLQ
     * visibility.
     */
    private AxoniqConsoleDlqMode dlqMode = AxoniqConsoleDlqMode.NONE;
    /**
     * The list which can be used in combination with setting the {@code dlqMode} to
     * {@link AxoniqConsoleDlqMode#LIMITED}. In that mode it will filter the diagnostics based on this list. It will use
     * the list as the keys to filter on.
     */
    private List<String> dlqDiagnosticsWhitelist = new ArrayList<>();
    /**
     * The access mode for Domain Events. Defaults to {@code NONE}, meaning no event payload is visible and loading
     * for aggregate reconstruction is disabled. If payload inspection is required, consider {@code PAYLOAD_ONLY}.
     * For full functionality, use {@code FULL}, which enables both payload visibility and aggregate snapshot loading.
     */
    private DomainEventAccessMode domainEventAccessMode = DomainEventAccessMode.NONE;
    /**
     * Whether the connection should use SSL/TLs. Defaults to {@code true}.
     */
    private boolean secure = true;
    /**
     * The initial delay before connecting to the AxonIQ Console in milliseconds. Defaults to {@code 0}.
     */
    private Long initialDelay = 0L;

    /**
     * The maximum number of concurrent management tasks. Defaults to {@code 5}. Management tasks are tasks executed at
     * the request of the user, such as processing DLQ messages.
     */
    private int maxConcurrentManagementTasks = 5;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getCredentials() {
        return credentials;
    }

    public void setCredentials(String credentials) {
        this.credentials = credentials;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public AxoniqConsoleDlqMode getDlqMode() {
        return dlqMode;
    }

    public void setDlqMode(AxoniqConsoleDlqMode dlqMode) {
        this.dlqMode = dlqMode;
    }

    public List<String> getDlqDiagnosticsWhitelist() {
        return dlqDiagnosticsWhitelist;
    }

    public void setDlqDiagnosticsWhitelist(List<String> dlqDiagnosticsWhitelist) {
        this.dlqDiagnosticsWhitelist = dlqDiagnosticsWhitelist;
    }

    public DomainEventAccessMode getDomainEventAccessMode() {
        return domainEventAccessMode;
    }

    public void setDomainEventAccessMode(DomainEventAccessMode domainEventAccessMode) {
        this.domainEventAccessMode = domainEventAccessMode;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public Long getInitialDelay() {
        return initialDelay;
    }

    public void setInitialDelay(Long initialDelay) {
        this.initialDelay = initialDelay;
    }

    public int getMaxConcurrentManagementTasks() {
        return maxConcurrentManagementTasks;
    }

    public void setMaxConcurrentManagementTasks(int maxConcurrentManagementTasks) {
        this.maxConcurrentManagementTasks = maxConcurrentManagementTasks;
    }
}
