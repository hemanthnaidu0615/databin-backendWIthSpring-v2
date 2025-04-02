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
@CrossOrigin(origins = "http://localhost:5173") // Add this line for CORS support
public class TablesController {

    @Autowired
    private StarTreeService starTreeService;

    @GetMapping("/recent-orders")
    public ResponseEntity<?> getRecentOrders() {
        try {
            String ordersQuery = "SELECT order_id, product_id, unit_price, order_type FROM orders ORDER BY order_date DESC LIMIT 5";
            List<List<Object>> ordersData = starTreeService.executeSqlQuery(ordersQuery);

            if (ordersData.isEmpty()) {
                return ResponseEntity.ok(Collections.singletonMap("message", "No recent orders found."));
            }

            Set<Integer> productIds = ordersData.stream()
                .map(order -> parseInteger(order.get(1)))
                .collect(Collectors.toSet());

            if (productIds.isEmpty()) return ResponseEntity.ok(ordersData);

            String productQuery = "SELECT id AS product_id, name AS product_name, category_id FROM products WHERE id IN ("
                                  + productIds.stream().map(String::valueOf).collect(Collectors.joining(",")) + ")";
            List<List<Object>> productData = starTreeService.executeSqlQuery(productQuery);

            Map<Integer, ProductInfo> productMap = productData.stream()
                .collect(Collectors.toMap(
                    row -> parseInteger(row.get(0)),
                    row -> new ProductInfo(row.get(1).toString(), parseInteger(row.get(2)))
                ));

            Set<Integer> categoryIds = productData.stream()
                .map(row -> parseInteger(row.get(2)))
                .collect(Collectors.toSet());

            String categoryQuery = "SELECT id AS category_id, name AS category_name FROM categories WHERE id IN ("
                                  + categoryIds.stream().map(String::valueOf).collect(Collectors.joining(",")) + ")";
            List<List<Object>> categoryData = starTreeService.executeSqlQuery(categoryQuery);

            Map<Integer, String> categoryMap = categoryData.stream()
                .collect(Collectors.toMap(
                    row -> parseInteger(row.get(0)),
                    row -> row.get(1).toString()
                ));

            Set<Integer> orderIds = ordersData.stream()
                .map(order -> parseInteger(order.get(0)))
                .collect(Collectors.toSet());

            String shipmentQuery = "SELECT order_id, shipment_status FROM shipment WHERE order_id IN ("
                                   + orderIds.stream().map(String::valueOf).collect(Collectors.joining(",")) + ")";
            List<List<Object>> shipmentData = starTreeService.executeSqlQuery(shipmentQuery);

            Map<Integer, String> shipmentMap = shipmentData.stream()
                .collect(Collectors.toMap(
                    row -> parseInteger(row.get(0)),
                    row -> row.get(1).toString()
                ));

            List<Map<String, Object>> enrichedOrders = new ArrayList<>();
            for (List<Object> order : ordersData) {
                int orderId = parseInteger(order.get(0));
                int productId = parseInteger(order.get(1));
                ProductInfo product = productMap.getOrDefault(productId, new ProductInfo("N/A", null));
                String categoryName = categoryMap.getOrDefault(product.categoryId, "N/A");
                String shipmentStatus = shipmentMap.getOrDefault(orderId, "Pending");

                Map<String, Object> enrichedOrder = new HashMap<>();
                enrichedOrder.put("order_id", order.get(0));
                enrichedOrder.put("product_name", product.name);
                enrichedOrder.put("category", categoryName);
                enrichedOrder.put("price", order.get(2));
                enrichedOrder.put("order_type", order.get(3));
                enrichedOrder.put("shipment_status", shipmentStatus);

                enrichedOrders.add(enrichedOrder);
            }

            return ResponseEntity.ok(enrichedOrders);
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Collections.singletonMap("error", "Failed to fetch recent orders"));
        }
    }

    private int parseInteger(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return ((Number) obj).intValue();
        try {
            return Integer.parseInt(obj.toString().trim());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer format: " + obj, e);
        }
    }

    static class ProductInfo {
        String name;
        Integer categoryId;

        public ProductInfo(String name, Integer categoryId) {
            this.name = name;
            this.categoryId = categoryId;
        }
    }
}
