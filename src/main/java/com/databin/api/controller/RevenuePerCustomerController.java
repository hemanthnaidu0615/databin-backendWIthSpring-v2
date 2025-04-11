package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/api/revenue")
@CrossOrigin(origins = "http://localhost:5173")
public class RevenuePerCustomerController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Top 7 Customers by Revenue (with date filter)
    @GetMapping("/top-customers")
    public ResponseEntity<?> getTopCustomersByRevenue(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate) {
        try {
            // ✅ Group by customer_id to use indexes better
            String query = String.format("""
                SELECT 
                    o.customer_id,
                    ROUND(SUM(o.total_amount), 2) AS total_revenue
                FROM orders o
                WHERE o.order_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
                GROUP BY o.customer_id
                ORDER BY total_revenue DESC
                LIMIT 7
            """, startDate, endDate);

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            List<Map<String, Object>> topCustomers = new ArrayList<>();

            for (List<Object> row : data) {
                String customerId = Objects.toString(row.get(0), "N/A");
                double revenue = parseDouble(row.get(1));

                // 🔄 Fetch name in Java or cache it (optional: add async batch fetch if needed)
                String customerName = getCustomerName(customerId); // Replace with actual resolver or cache

                topCustomers.add(Map.of(
                    "customer_id", customerId,
                    "customer_name", customerName,
                    "revenue", revenue
                ));
            }

            return ResponseEntity.ok(Map.of("top_customers", topCustomers));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch top customers by revenue"));
        }
    }

    // 🔧 Dummy name resolver (replace with actual DB call or cache)
    private String getCustomerName(String customerId) {
        // Ideally this is a Redis cache or batched lookup.
        // Placeholder return:
        return "Customer " + customerId;
    }


    // 🔹 Helper Method: Convert Object to Double
    private double parseDouble(Object obj) {
        if (obj == null) return 0.0;
        if (obj instanceof Number) return ((Number) obj).doubleValue();
        try {
            return Double.parseDouble(obj.toString());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid double format: " + obj, e);
        }
    }
}
