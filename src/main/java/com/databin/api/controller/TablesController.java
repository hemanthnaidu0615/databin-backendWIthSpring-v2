package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/api/orders")
@CrossOrigin(origins = "http://localhost:5173")
public class TablesController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 Optimized API: Get Recent Orders with JOINs (faster version)
    @GetMapping("/recent-orders")
    public ResponseEntity<?> getRecentOrders(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate) {
        try {
            // ✅ Combine all data fetching in one optimized query
            String query = String.format("""
                SELECT 
                    o.order_id,
                    p.name AS product_name,
                    c.name AS category_name,
                    o.unit_price,
                    o.order_type,
                    COALESCE(s.shipment_status, 'Pending') AS shipment_status
                FROM orders o
                LEFT JOIN products p ON o.product_id = p.id
                LEFT JOIN categories c ON p.category_id = c.id
                LEFT JOIN shipment s ON o.order_id = s.order_id
                WHERE o.order_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
                ORDER BY o.order_date DESC
                LIMIT 5
            """, startDate, endDate);

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            if (data.isEmpty()) {
                return ResponseEntity.ok(Collections.singletonMap("message", "No recent orders found."));
            }

            List<Map<String, Object>> orders = new ArrayList<>();
            for (List<Object> row : data) {
                Map<String, Object> order = new HashMap<>();
                order.put("order_id", row.get(0));
                order.put("product_name", row.get(1) != null ? row.get(1).toString() : "N/A");
                order.put("category", row.get(2) != null ? row.get(2).toString() : "N/A");
                order.put("price", row.get(3));
                order.put("order_type", row.get(4));
                order.put("shipment_status", row.get(5));
                orders.add(order);
            }

            return ResponseEntity.ok(orders);

        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Collections.singletonMap("error", "Failed to fetch recent orders"));
        }
    }
}
