package com.hieunt.stock.filter;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RateLimitFilter extends OncePerRequestFilter {

    private final LoadingCache<String, Bucket> apiBuckets;
    private final LoadingCache<String, Bucket> publicBuckets;

    public RateLimitFilter() {
        this.apiBuckets = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterAccess(50, TimeUnit.MINUTES)
                .build(key -> createNewBucket());

        this.publicBuckets = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterAccess(50, TimeUnit.MINUTES)
                .build(key -> createNewBucket());
    }

    private Bucket createNewBucket() {
        // 1 request per minute
        // Refill.greedy ensures tokens are added smoothly over the period if capacity >
        // 1,
        // but for capacity 1 it behaves simply as a reset every minute relative to
        // consumption.
        Bandwidth limit = Bandwidth.classic(2, Refill.greedy(1, Duration.ofMinutes(1)));
        return Bucket.builder().addLimit(limit).build();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String path = request.getRequestURI();
        String ip = request.getRemoteAddr();
        Bucket bucket = null;

        if (path.startsWith("/api/")) {
            bucket = apiBuckets.get(ip);
        } else if (path.startsWith("/public/")) {
            bucket = publicBuckets.get(ip);
        }

        if (bucket != null) {
            if (bucket.tryConsume(1)) {
                filterChain.doFilter(request, response);
            } else {
                response.setStatus(429); // Too Many Requests
                response.setContentType("application/json");
                response.getWriter().write("{ \"message\": \"Too many requests\" }");
            }
        } else {
            filterChain.doFilter(request, response);
        }
    }
}
