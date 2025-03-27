package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/orders")
public class TablesController {

    @Autowired
    private StarTreeService starTreeService;

    @GetMapping("/recent-orders")
    public ResponseEntity<?> getRecentOrders() {
        try {
            // 🟢 Step 1: Fetch Latest 5 Orders
            String ordersQuery = "SELECT order_id, product_id, unit_price, order_type FROM orders ORDER BY order_date DESC LIMIT 5";
            List<List<Object>> ordersData = starTreeService.executeSqlQuery(ordersQuery);

            if (ordersData.isEmpty()) {
                return ResponseEntity.ok(Collections.singletonMap("message", "No recent orders found."));
            }

            // ✅ Fix: Use safer type conversion for product IDs
            Set<Integer> productIds = ordersData.stream()
                .map(order -> parseInteger(order.get(1)))  // Product ID is at index 1
                .collect(Collectors.toSet());

            // 🟢 Step 2: Fetch Product Data
            if (productIds.isEmpty()) return ResponseEntity.ok(ordersData);

            String productQuery = "SELECT id AS product_id, name AS product_name, category_id FROM products WHERE id IN (" 
                                  + productIds.stream().map(String::valueOf).collect(Collectors.joining(",")) + ")";
            List<List<Object>> productData = starTreeService.executeSqlQuery(productQuery);

            Map<Integer, ProductInfo> productMap = productData.stream()
                .collect(Collectors.toMap(
                    row -> parseInteger(row.get(0)), 
                    row -> new ProductInfo(row.get(1).toString(), parseInteger(row.get(2)))
                ));

            // 🟢 Step 3: Fetch Category Data
            Set<Integer> categoryIds = productData.stream()
                .map(row -> parseInteger(row.get(2)))  // Category ID is at index 2
                .collect(Collectors.toSet());

            String categoryQuery = "SELECT id AS category_id, name AS category_name FROM categories WHERE id IN (" 
                                  + categoryIds.stream().map(String::valueOf).collect(Collectors.joining(",")) + ")";
            List<List<Object>> categoryData = starTreeService.executeSqlQuery(categoryQuery);

            Map<Integer, String> categoryMap = categoryData.stream()
                .collect(Collectors.toMap(
                    row -> parseInteger(row.get(0)), 
                    row -> row.get(1).toString()
                ));

            // 🟢 Step 4: Merge Data
            List<Map<String, Object>> enrichedOrders = new ArrayList<>();
            for (List<Object> order : ordersData) {
                int productId = parseInteger(order.get(1));
                ProductInfo product = productMap.getOrDefault(productId, new ProductInfo("N/A", null));
                String categoryName = categoryMap.getOrDefault(product.categoryId, "N/A");

                Map<String, Object> enrichedOrder = new HashMap<>();
                enrichedOrder.put("order_id", order.get(0));
                enrichedOrder.put("product_name", product.name);
                enrichedOrder.put("category", categoryName);
                enrichedOrder.put("price", order.get(2));
                enrichedOrder.put("order_type", order.get(3));

                enrichedOrders.add(enrichedOrder);
            }

            return ResponseEntity.ok(enrichedOrders);
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Collections.singletonMap("error", "Failed to fetch recent orders"));
        }
    }

    // ✅ Helper Method: Convert Object to Integer Safely
    private int parseInteger(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return ((Number) obj).intValue();
        try {
            return Integer.parseInt(obj.toString().trim());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer format: " + obj, e);
        }
    }

    // ✅ Helper Class for Product Data
    static class ProductInfo {
        String name;
        Integer categoryId;

        public ProductInfo(String name, Integer categoryId) {
            this.name = name;
            this.categoryId = categoryId;
        }
    }
}
