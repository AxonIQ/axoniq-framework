/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.framework.extension.dataprotection.sample.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.router

/**
 * Spring WebFlux configuration for serving static web resources.
 *
 * This configuration class addresses the fundamental difference between Spring MVC and
 * Spring WebFlux regarding static resource handling. Unlike Spring MVC, which automatically
 * serves static resources from src/main/resources/static, WebFlux requires explicit
 * configuration to handle static file serving.
 *
 * Problem solved:
 * WebFlux applications don't automatically serve static resources, causing 404 errors
 * when trying to access HTML, CSS, JavaScript, and image files. This configuration
 * provides a reactive solution using functional routing.
 *
 * Architecture choice:
 * This implementation uses Spring WebFlux's functional routing approach rather than
 * annotation-based controllers, which aligns with the reactive programming model and
 * provides better performance for static resource serving.
 *
 * Routing strategy:
 * - Root path ("/"): Serves the main index.html file
 * - File paths ("/{filename}"): Serves any requested static resource
 * - Content-Type detection: Automatically sets appropriate MIME types
 * - 404 handling: Returns proper HTTP 404 for missing resources
 *
 * Supported file types:
 * - HTML files (text/html)
 * - CSS files (text/css)
 * - JavaScript files (text/javascript)
 * - Images and other files (application/octet-stream)
 *
 * Security considerations:
 * - Files are served only from the classpath static directory
 * - No directory traversal vulnerabilities due to ClassPathResource usage
 * - Proper content-type headers prevent MIME-type confusion attacks
 *
 * Performance characteristics:
 * - Non-blocking I/O through WebFlux reactive streams
 * - Efficient resource handling via Spring's ClassPathResource
 * - Minimal memory footprint for static file serving
 *
 */
@Configuration
class WebConfig {

    /**
     * Creates a functional router for serving static web resources.
     *
     * This bean defines the routing rules for static resource requests using
     * Spring WebFlux's functional routing API. It handles both the root path
     * (serving index.html) and individual file requests with proper content-type
     * detection and 404 error handling.
     *
     * Route definitions:
     * 1. GET "/" → serves static/index.html as text/html
     * 2. GET "/{filename}" → serves static/{filename} with detected content-type
     *
     * Implementation details:
     * - Uses ClassPathResource for secure classpath-only access
     * - Employs regex pattern /{filename:.+} to match any filename with extension
     * - Automatically detects and sets appropriate MIME types
     * - Returns HTTP 404 for non-existent resources
     *
     * @return RouterFunction that handles static resource requests
     */
    @Bean
    fun staticResourceRouter(): RouterFunction<ServerResponse> = router {
        GET("/") {
            ServerResponse.ok()
                .contentType(MediaType.TEXT_HTML)
                .bodyValue(ClassPathResource("static/index.html"))
        }
        GET("/{filename:.+}") { request ->
            val filename = request.pathVariable("filename")
            val resource = ClassPathResource("static/$filename")

            if (resource.exists()) {
                val mediaType = getMediaType(filename)
                ServerResponse.ok()
                    .contentType(mediaType)
                    .bodyValue(resource)
            } else {
                ServerResponse.notFound().build()
            }
        }
    }

    /**
     * Determines the appropriate MIME type based on file extension.
     *
     * This utility method provides content-type detection for common web resource
     * file types, ensuring that browsers receive proper MIME type headers for
     * correct rendering and security.
     *
     * Supported extensions:
     * - .html → text/html
     * - .css → text/css
     * - .js → text/javascript
     * - others → application/octet-stream (binary default)
     *
     * Security benefit:
     * Setting correct content-type headers prevents browsers from performing
     * MIME-type sniffing, which could lead to security vulnerabilities when
     * user-uploaded content is served.
     *
     * Extensibility:
     * Additional file types can be easily added by extending the when expression
     * or by implementing a more sophisticated mapping strategy for production use.
     *
     * @param filename the name of the file including its extension
     * @return the appropriate MediaType for the file
     */
    private fun getMediaType(filename: String): MediaType {
        return when {
            filename.endsWith(".html") -> MediaType.TEXT_HTML
            filename.endsWith(".css") -> MediaType.valueOf("text/css")
            filename.endsWith(".js") -> MediaType.valueOf("text/javascript")
            else -> MediaType.APPLICATION_OCTET_STREAM
        }
    }
}
