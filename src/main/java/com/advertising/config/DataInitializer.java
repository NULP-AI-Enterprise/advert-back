package com.advertising.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Application startup initializer.
 * V1: Embedding enrichment has been removed — recommendations use SQL-based retrieval.
 */
@Slf4j
@Component
public class DataInitializer implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
        log.info("[Init] V1 startup complete. SQL-based recommendation engine ready.");
    }
}
