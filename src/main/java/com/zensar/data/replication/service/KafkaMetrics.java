package com.zensar.data.replication.service;

import org.springframework.stereotype.Service;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Instant;

@Service
public class KafkaMetrics {

    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
    private final AtomicLong totalRetries = new AtomicLong(0);
    private final AtomicLong totalFailures = new AtomicLong(0);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private volatile Instant startTime = Instant.now();

    public void recordProcessing(long processingTimeMs) {
        totalProcessed.incrementAndGet();
        totalProcessingTimeMs.addAndGet(processingTimeMs);
    }

    public void recordRetry() {
        totalRetries.incrementAndGet();
    }

    public void recordFailure() {
        totalFailures.incrementAndGet();
    }

    public void stop() {
        isRunning.set(false);
    }

    public void reset() {
        totalProcessed.set(0);
        totalProcessingTimeMs.set(0);
        totalRetries.set(0);
        totalFailures.set(0);
        isRunning.set(true);
        startTime = Instant.now();
    }

    // Getters
    public long getTotalProcessed() { return totalProcessed.get(); }
    public long getTotalProcessingTimeMs() { return totalProcessingTimeMs.get(); }
    public long getTotalRetries() { return totalRetries.get(); }
    public long getTotalFailures() { return totalFailures.get(); }
    public boolean isRunning() { return isRunning.get(); }

    public long getRuntimeSeconds() {
        return Instant.now().getEpochSecond() - startTime.getEpochSecond();
    }

    public double getTPS() {
        long runtime = getRuntimeSeconds();
        return runtime > 0 ? (double) totalProcessed.get() / runtime : 0.0;
    }

    public double getAvgProcessingTimeMs() {
        long processed = totalProcessed.get();
        return processed > 0 ? (double) totalProcessingTimeMs.get() / processed : 0.0;
    }
}
