package com.databin.api.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.databin.api.service.StarTreeService;

@RestController
@RequestMapping("/api/shipment-status")
public class ShipmentStatusController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Order Count by Shipment Status
    @GetMapping("/count")
    public ResponseEntity<?> getOrderStatusCount() {
        try {
            // Query to fetch Delivered count from shipment table
            String deliveredQuery = """
                SELECT COUNT(*) 
                FROM shipment 
                WHERE shipment_status = 'Delivered'
            """;

            // Query to fetch Shipped, Pending, Cancelled, and Return Received counts from fulfillment_event table
            String fulfillmentQuery = """
                SELECT event_type, COUNT(*) 
                FROM fulfillment_event 
                WHERE event_type IN ('Shipped', 'Pending', 'Cancelled', 'Return Received') 
                GROUP BY event_type
            """;

            // Execute queries
            List<List<Object>> deliveredData = starTreeService.executeSqlQuery(deliveredQuery);
            List<List<Object>> fulfillmentData = starTreeService.executeSqlQuery(fulfillmentQuery);

            // Initialize a map with default values
            Map<String, Integer> statusCounts = new HashMap<>();
            statusCounts.put("Delivered", deliveredData.isEmpty() ? 0 : parseInteger(deliveredData.get(0).get(0)));
            statusCounts.put("Shipped", 0);
            statusCounts.put("Pending", 0);
            statusCounts.put("Cancelled", 0);
            statusCounts.put("Return Received", 0);
            statusCounts.put("Refunded", 0); // Will be calculated

            // Populate the map with actual fulfillment data
            int returnReceivedCount = 0;
            for (List<Object> row : fulfillmentData) {
                String status = row.get(0).toString();
                int count = parseInteger(row.get(1));
                statusCounts.put(status, count);
                if ("Return Received".equals(status)) {
                    returnReceivedCount = count;
                }
            }

            // Calculate "Refunded" as Return Received / 3 (rounded)
            statusCounts.put("Refunded", Math.round(returnReceivedCount / 3.0f));

            return ResponseEntity.ok(statusCounts);

        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch order status counts"));
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
}
