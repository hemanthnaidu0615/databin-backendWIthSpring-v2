package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/api/top-sellers")
@CrossOrigin(origins = "http://localhost:5173")
public class TopSellingProductsController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Top 5 Selling Products with Percentages (with date filter)
    @GetMapping("/top-products")
    public ResponseEntity<?> getTopSellingProducts(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate) {

        try {
            String query =
                "SELECT " +
                "    p.name AS product_name, " +
                "    SUM(o.quantity) AS total_quantity, " +
                "    ROUND(SUM(o.quantity) * 100.0 / " +
                "        (SELECT SUM(quantity) FROM orders WHERE order_date BETWEEN TIMESTAMP '" + startDate + "' AND TIMESTAMP '" + endDate + "'), 2) AS percentage " +
                "FROM orders o " +
                "JOIN products p ON o.product_id = p.id " +
                "WHERE o.order_date BETWEEN TIMESTAMP '" + startDate + "' AND TIMESTAMP '" + endDate + "' " +
                "GROUP BY p.name " +
                "ORDER BY total_quantity DESC " +
                "LIMIT 5";

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            List<Map<String, Object>> topProducts = new ArrayList<>();

            for (List<Object> row : data) {
                String name = Objects.toString(row.get(0), "N/A");
                int quantity = parseInteger(row.get(1));
                double percent = parseDouble(row.get(2));

                topProducts.add(Map.of(
                    "product_name", name,
                    "quantity_sold", quantity,
                    "percentage", String.format("%.2f%%", percent)
                ));
            }

            return ResponseEntity.ok(Map.of("top_products", topProducts));

        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch top selling products"));
        }
    }

    // 🔹 Helper Method: Convert Object to Integer
    private int parseInteger(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return ((Number) obj).intValue();
        try {
            return Integer.parseInt(obj.toString());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer format: " + obj, e);
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
