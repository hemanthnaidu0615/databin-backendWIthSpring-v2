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
@RequestMapping("/api/sales")
@CrossOrigin(origins = "http://localhost:5173")
public class SalesController {

    @Autowired
    private StarTreeService starTreeService;

    @GetMapping("/metrics")
    public ResponseEntity<?> getSalesMetrics(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate) {
        try {
            String metricsQuery = String.format("""
                WITH customer_orders AS (
                    SELECT customer_id, COUNT(order_id) AS orders_count, SUM(total_amount) AS total_spent
                    FROM orders
                    WHERE order_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
                    GROUP BY customer_id
                )
                SELECT
                    (SELECT ROUND(AVG(total_amount), 2) FROM orders
                     WHERE order_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s') AS avg_order_value,
                     
                    (SELECT COUNT(*) FROM customer_orders WHERE total_spent > 95578) AS high_spenders,
                    
                    (SELECT COUNT(*) FROM customer_orders WHERE orders_count = 1) AS new_customers,
                    
                    (SELECT COUNT(*) FROM customer_orders WHERE orders_count > 1) AS returning_customers
                """, startDate, endDate, startDate, endDate);

            List<List<Object>> result = starTreeService.executeSqlQuery(metricsQuery);

            if (result.isEmpty() || result.get(0).isEmpty()) {
                return ResponseEntity.ok(Collections.singletonMap("message", "No data found for given range"));
            }

            List<Object> row = result.get(0);
            Map<String, Object> response = new HashMap<>();
            response.put("avg_order_value", parseDouble(row.get(0)));
            response.put("high_spenders", parseInteger(row.get(1)));
            response.put("new_customers", parseInteger(row.get(2)));
            response.put("returning_customers", parseInteger(row.get(3)));

            return ResponseEntity.ok(response);

        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Collections.singletonMap("error", "Failed to fetch sales metrics"));
        }
    }

    // ✅ Helper: Convert Object to Double Safely
    private double parseDouble(Object obj) {
        if (obj instanceof Number) return ((Number) obj).doubleValue();
        try {
            return Double.parseDouble(obj.toString().trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    // ✅ Helper: Convert Object to Integer Safely
    private int parseInteger(Object obj) {
        if (obj instanceof Number) return ((Number) obj).intValue();
        try {
            return Integer.parseInt(obj.toString().trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
