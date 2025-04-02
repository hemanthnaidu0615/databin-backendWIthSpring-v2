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
    public ResponseEntity<?> getSalesMetrics() {
        try {
            // 🟢 Query for Average Order Value
            String avgOrderQuery = "SELECT ROUND(AVG(total_amount), 2) AS avg_order_value FROM orders";

            // 🟢 Query for High Spenders
            String highSpendersQuery = "SELECT COUNT(DISTINCT customer_id) AS high_spenders FROM ("
                    + "SELECT customer_id, SUM(total_amount) AS total_spent FROM orders "
                    + "GROUP BY customer_id HAVING SUM(total_amount) > 95578) AS high_spender_customers";

            // 🟢 Query for New Customers (Customers with only 1 order)
            String newCustomersQuery = "SELECT COUNT(customer_id) AS new_customers FROM ("
                    + "SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(order_id) = 1) AS new_customers";

            // 🟢 Query for Returning Customers (Customers with more than 1 order)
            String returningCustomersQuery = "SELECT COUNT(customer_id) AS returning_customers FROM ("
                    + "SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(order_id) > 1) AS returning_customers";

            // 🟢 Execute all queries asynchronously
            CompletableFuture<List<List<Object>>> avgOrderFuture = executeQueryAsync(avgOrderQuery);
            CompletableFuture<List<List<Object>>> highSpendersFuture = executeQueryAsync(highSpendersQuery);
            CompletableFuture<List<List<Object>>> newCustomersFuture = executeQueryAsync(newCustomersQuery);
            CompletableFuture<List<List<Object>>> returningCustomersFuture = executeQueryAsync(returningCustomersQuery);

            // 🟢 Wait for all queries to complete
            CompletableFuture.allOf(avgOrderFuture, highSpendersFuture, newCustomersFuture, returningCustomersFuture).join();

            // 🟢 Extract values safely
            double avgOrderValue = extractDouble(avgOrderFuture.get());
            int highSpenders = extractInteger(highSpendersFuture.get());
            int newCustomers = extractInteger(newCustomersFuture.get());
            int returningCustomers = extractInteger(returningCustomersFuture.get());

            // 🟢 Build response
            Map<String, Object> response = new HashMap<>();
            response.put("avg_order_value", avgOrderValue);
            response.put("high_spenders", highSpenders);
            response.put("new_customers", newCustomers);
            response.put("returning_customers", returningCustomers);

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

    // ✅ Helper: Extract Double from Query Result
    private double extractDouble(List<List<Object>> result) {
        if (result != null && !result.isEmpty() && result.get(0).size() > 0) {
            return parseDouble(result.get(0).get(0));
        }
        return 0.0;
    }

    // ✅ Helper: Extract Integer from Query Result
    private int extractInteger(List<List<Object>> result) {
        if (result != null && !result.isEmpty() && result.get(0).size() > 0) {
            return parseInteger(result.get(0).get(0));
        }
        return 0;
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
