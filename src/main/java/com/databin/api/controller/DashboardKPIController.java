package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dashboard-kpi")
public class DashboardKPIController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Total Orders Count
    @GetMapping("/total-orders")
    public ResponseEntity<?> getTotalOrders() {
        try {
            String query = "SELECT COUNT(*) FROM orders";
            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            int totalOrders = data.isEmpty() ? 0 : parseInteger(data.get(0).get(0));
            return ResponseEntity.ok(Map.of("total_orders", totalOrders));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch total orders"));
        }
    }

    // 📌 API: Get Shipment Status Percentages
    @GetMapping("/shipment-status-percentage")
    public ResponseEntity<?> getShipmentStatusPercentage() {
        try {
            String query = """
                SELECT 
                    COUNT(*) AS total_orders,
                    SUM(CASE WHEN shipment_status = 'Delayed' THEN 1 ELSE 0 END) AS delayed_orders,
                    SUM(CASE WHEN shipment_status = 'In Transit' THEN 1 ELSE 0 END) AS in_transit_orders
                FROM shipment
            """;

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            if (data.isEmpty() || data.get(0).get(0) == null) {
                return ResponseEntity.ok(Map.of("delayed_percentage", "0.00%", "in_transit_orders", 0));
            }

            int totalOrders = parseInteger(data.get(0).get(0));
            int delayedOrders = parseInteger(data.get(0).get(1));
            int inTransitOrders = parseInteger(data.get(0).get(2));

            double delayedPercentage = (totalOrders > 0) ? (delayedOrders * 100.0 / totalOrders) : 0.0;

            return ResponseEntity.ok(Map.of(
                "delayed_percentage", String.format("%.2f%%", delayedPercentage),
                "in_transit_orders", inTransitOrders
            ));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch shipment data"));
        }
    }

    // 📌 API: Get Fulfillment Rate (already fixed)
    @GetMapping("/fulfillment-rate")
    public ResponseEntity<?> getFulfillmentRate() {
        try {
            String query = """
                SELECT 
                    (COUNT(e.order_id) * 100.0 / NULLIF(COUNT(o.order_id), 0)) AS fulfillment_rate
                FROM orders o
                LEFT JOIN fulfillment_event e 
                ON o.order_id = e.order_id
                AND e.event_type IN ('Shipped', 'Same-Day Delivery', 'Ship to Home', 
                                     'Store Pickup', 'Curbside Pickup', 'Locker Pickup')
            """;

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            if (data.isEmpty() || data.get(0).get(0) == null) {
                return ResponseEntity.ok(Map.of("message", "No fulfillment data available."));
            }

            double fulfillmentRate = parseDouble(data.get(0).get(0));

            return ResponseEntity.ok(Map.of("fulfillment_rate", String.format("%.2f%%", fulfillmentRate)));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch fulfillment rate"));
        }
    }

    // 📌 API: Get Out-of-Stock Product Count
    @GetMapping("/out-of-stock")
    public ResponseEntity<?> getOutOfStockCount() {
        try {
            String query = """
                SELECT COUNT(DISTINCT product_id) AS out_of_stock_count
                FROM inventory
                WHERE status = 'Out of Stock'
            """;

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            int outOfStockCount = data.isEmpty() ? 0 : parseInteger(data.get(0).get(0));

            return ResponseEntity.ok(Map.of("out_of_stock_count", outOfStockCount));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch out-of-stock count"));
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
