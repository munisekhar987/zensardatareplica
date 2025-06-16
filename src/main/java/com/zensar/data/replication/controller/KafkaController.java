package com.zensar.data.replication.controller;

import com.zensar.data.replication.service.KafkaMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    @Autowired
    private KafkaMetrics metrics;

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", metrics.isRunning() ? "RUNNING" : "STOPPED");
        response.put("totalProcessed", metrics.getTotalProcessed());
        response.put("totalProcessingTimeMs", metrics.getTotalProcessingTimeMs());
        response.put("avgProcessingTimeMs", String.format("%.2f", metrics.getAvgProcessingTimeMs()));
        response.put("totalRetries", metrics.getTotalRetries());
        response.put("totalFailures", metrics.getTotalFailures());
        response.put("runtimeSeconds", metrics.getRuntimeSeconds());
        response.put("tps", String.format("%.2f", metrics.getTPS()));
        return response;
    }

    @PostMapping("/reset")
    public String reset() {
        metrics.reset();
        return "Metrics reset successfully";
    }

    @GetMapping("/tps")
    public double getTPS() {
        return Double.parseDouble(String.format("%.2f", metrics.getTPS()));
    }
}