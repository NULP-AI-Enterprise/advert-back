package com.advertising.service.geo;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a client IP address to a country name via ip-api.com (free tier, HTTP only).
 * Results are cached in-memory — IP→country mappings are stable enough to cache indefinitely
 * for the lifetime of the process.
 */
@Slf4j
@Service
public class IpGeoService {

    private final WebClient webClient = WebClient.builder()
        .baseUrl("http://ip-api.com")
        .codecs(c -> c.defaultCodecs().maxInMemorySize(64 * 1024))
        .build();

    private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();

    /**
     * Returns the country name for the given IP, or empty if the IP is private/loopback
     * or the lookup fails. Never propagates errors — callers always get empty on failure.
     */
    public Mono<String> getCountry(String ip) {
        if (ip == null || ip.isBlank() || isPrivate(ip)) {
            return Mono.empty();
        }

        String cached = cache.get(ip);
        if (cached != null) {
            return Mono.just(cached);
        }

        return webClient.get()
            .uri("/json/{ip}?fields=status,country", ip)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .timeout(Duration.ofSeconds(3))
            .flatMap(node -> {
                if (!"success".equals(node.path("status").asText())) {
                    return Mono.<String>empty();
                }
                String country = node.path("country").asText(null);
                if (country != null && !country.isBlank()) {
                    cache.put(ip, country);
                    log.debug("[GeoIP] {} → {}", ip, country);
                    return Mono.just(country);
                }
                return Mono.<String>empty();
            })
            .onErrorResume(e -> {
                log.debug("[GeoIP] lookup failed for {}: {}", ip, e.getMessage());
                return Mono.empty();
            });
    }

    /**
     * Extracts the real client IP from WebSocket handshake headers (X-Forwarded-For,
     * X-Real-IP) or falls back to the TCP remote address.
     */
    public static String extractIp(HttpHeaders handshakeHeaders, InetSocketAddress remoteAddress) {
        // X-Forwarded-For may be a comma-separated list; the leftmost entry is the real client
        String xff = handshakeHeaders.getFirst("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            String first = xff.split(",")[0].trim();
            if (!first.isEmpty()) return first;
        }
        String xrip = handshakeHeaders.getFirst("X-Real-IP");
        if (xrip != null && !xrip.isBlank()) return xrip.trim();

        return remoteAddress != null ? remoteAddress.getAddress().getHostAddress() : null;
    }

    private boolean isPrivate(String ip) {
        if (ip.startsWith("127.") || ip.startsWith("10.")
                || ip.startsWith("192.168.") || ip.equals("::1")
                || ip.equals("0:0:0:0:0:0:0:1")) {
            return true;
        }
        // 172.16.0.0 – 172.31.255.255
        if (ip.startsWith("172.")) {
            try {
                int second = Integer.parseInt(ip.split("\\.")[1]);
                return second >= 16 && second <= 31;
            } catch (Exception ignored) {}
        }
        return false;
    }
}
