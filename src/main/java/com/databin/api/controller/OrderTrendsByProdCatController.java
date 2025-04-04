package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/api/order-trends-by-category")
@CrossOrigin(origins = "http://localhost:5173")
public class OrderTrendsByProdCatController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Monthly Order Trends by Product Category
    @GetMapping
    public ResponseEntity<?> getOrderTrendsByCategory() {
        try {
            String query = """
                SELECT 
                    SUBSTRING(order_date, 1, 7) AS month,
                    c.name AS category,
                    SUM(o.quantity * o.unit_price) AS total_sales
                FROM orders o
                JOIN products p ON o.product_id = p.id
                JOIN categories c ON p.category_id = c.id
                GROUP BY month, category
                ORDER BY month
            """;

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            // 🧩 Transforming Result into a Nested Map Format
            Map<String, Map<String, Double>> result = new LinkedHashMap<>();

            for (List<Object> row : data) {
                String month = Objects.toString(row.get(0), "Unknown");
                String category = Objects.toString(row.get(1), "Unknown");
                double sales = parseDouble(row.get(2));

                result.computeIfAbsent(month, k -> new LinkedHashMap<>())
                      .put(category, sales);
            }

            return ResponseEntity.ok(Map.of("order_trends", result));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch order trends data"));
        }
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
