package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/orders")
@CrossOrigin(origins = "http://localhost:5173")
public class OrderTrendsController {

    @Autowired
    private StarTreeService starTreeService;

    @GetMapping("/trends/weekly")
    public ResponseEntity<?> getOrderTrendsByCategoryWeekly() {
        try {
            // 🟢 Query for Weekly Order Trends by Product Category
            String orderTrendsQuery = "SELECT DATE_TRUNC('WEEK', order_date) AS week_start, "
                    + "p.category_id, COUNT(o.order_id) AS order_count "
                    + "FROM orders o "
                    + "JOIN products p ON o.product_id = p.product_id "
                    + "GROUP BY week_start, p.category_id "
                    + "ORDER BY week_start ASC";

            // 🟢 Execute query asynchronously
            CompletableFuture<List<List<Object>>> orderTrendsFuture = executeQueryAsync(orderTrendsQuery);

            // 🟢 Wait for query to complete
            CompletableFuture.allOf(orderTrendsFuture).join();

            // 🟢 Extract data safely
            List<List<Object>> orderTrendsResult = orderTrendsFuture.get();
            List<Map<String, Object>> trendsData = new ArrayList<>();

            for (List<Object> row : orderTrendsResult) {
                Map<String, Object> trend = new HashMap<>();
                trend.put("week_start", row.get(0));
                trend.put("category_id", row.get(1));
                trend.put("order_count", row.get(2));
                trendsData.add(trend);
            }

            // 🟢 Build response
            Map<String, Object> response = new HashMap<>();
            response.put("weekly_order_trends", trendsData);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Collections.singletonMap("error", "Internal Server Error"));
        }
    }

    // ✅ Helper: Execute Query Asynchronously
    private CompletableFuture<List<List<Object>>> executeQueryAsync(String query) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return starTreeService.executeSqlQuery(query);
            } catch (IOException e) {
                throw new RuntimeException("Error executing query", e);
            }
        });
    }
}
