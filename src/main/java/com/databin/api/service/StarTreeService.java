package com.databin.api.service;

import com.databin.api.config.StarTreeConfig;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class StarTreeService {
    
    private static final Logger logger = LoggerFactory.getLogger(StarTreeService.class);
    private final StarTreeConfig config;
    private final ObjectMapper objectMapper;

    @Autowired
    public StarTreeService(StarTreeConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
    }

    public List<List<Object>> executeSqlQuery(String sql) throws IOException {
        // Validate SQL Query
        if (sql == null || sql.trim().isEmpty()) {
            logger.error("❌ SQL Query is null or empty.");
            throw new IllegalArgumentException("SQL Query cannot be null or empty.");
        }

        // Remove newline characters from the SQL query
        String sanitizedSql = sql.replaceAll("[\\n\\r]", " ");

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(config.getBrokerUrl());

            // Set Headers
            post.setHeader("Authorization", "Bearer " + config.getAuthToken());
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Accept", "application/json");
            post.setHeader("database", "ws_2tcna4wdsaga");

            // Create request body with the sanitized SQL query
            String jsonBody = String.format("{\"sql\":\"%s\", \"queryOptions\":\"useMultistageEngine=true\"}", sanitizedSql);
            HttpEntity entity = new StringEntity(jsonBody, ContentType.APPLICATION_JSON);
            post.setEntity(entity);

            // Log request details
            logger.info("🔹 Sending SQL Query: {}", sanitizedSql);
            logger.info("🔹 Broker URL: {}", config.getBrokerUrl());

            return client.execute(post, response -> {
                if (response.getEntity() == null) {
                    logger.error("❌ Pinot Response: EMPTY");
                    return new ArrayList<>();
                }

                // Read response
                try (InputStream responseStream = response.getEntity().getContent()) {
                    String jsonResponse = new String(responseStream.readAllBytes(), StandardCharsets.UTF_8);
                    logger.debug("✅ Raw Pinot Response: {}", jsonResponse);
                    return parsePinotResponse(jsonResponse);
                }
            });
        }
    }

    private List<List<Object>> parsePinotResponse(String jsonResponse) {
        try {
            JsonNode rootNode = objectMapper.readTree(jsonResponse);

            // Handle permission errors (403)
            if (rootNode.has("code") && rootNode.get("code").asInt() == 403) {
                logger.error("❌ Permission Denied: {}", rootNode.get("error").asText());
                return new ArrayList<>();
            }

            // Handle API errors
            if (rootNode.has("error")) {
                logger.error("❌ Pinot API Error: {}", rootNode.get("error").asText());
                return new ArrayList<>();
            }

            List<List<Object>> results = new ArrayList<>();
            Optional.ofNullable(rootNode.path("resultTable").path("rows"))
                    .filter(JsonNode::isArray)
                    .ifPresent(rows -> rows.forEach(row -> {
                        List<Object> parsedRow = new ArrayList<>();
                        row.forEach(value -> parsedRow.add(value.isNull() ? null : value.asText()));
                        results.add(parsedRow);
                    }));

            return results;
        } catch (Exception e) {
            logger.error("❌ Error parsing response: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }
}
