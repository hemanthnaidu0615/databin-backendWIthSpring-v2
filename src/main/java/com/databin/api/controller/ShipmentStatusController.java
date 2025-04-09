package com.databin.api.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.databin.api.service.StarTreeService;

@RestController
@RequestMapping("/api/shipment-status")
@CrossOrigin(origins = "http://localhost:5173")
public class ShipmentStatusController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Order Count by Shipment Status (with date filtering)
    @GetMapping("/count")
    public ResponseEntity<?> getOrderStatusCount(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate) {
        try {
            // Delivered count from shipment table with date filter
            String deliveredQuery = String.format("""
                SELECT COUNT(*) 
                FROM shipment 
                WHERE shipment_status = 'Delivered' 
                AND actual_delivery_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
            """, startDate, endDate);

            // Fulfillment events count with date filter
            String fulfillmentQuery = String.format("""
                SELECT event_type, COUNT(*) 
                FROM fulfillment_event 
                WHERE event_type IN ('Shipped', 'Pending', 'Cancelled', 'Return Received') 
                AND event_time BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
                GROUP BY event_type
            """, startDate, endDate);

            List<List<Object>> deliveredData = starTreeService.executeSqlQuery(deliveredQuery);
            List<List<Object>> fulfillmentData = starTreeService.executeSqlQuery(fulfillmentQuery);

            Map<String, Integer> statusCounts = new HashMap<>();
            statusCounts.put("Delivered", deliveredData.isEmpty() ? 0 : parseInteger(deliveredData.get(0).get(0)));
            statusCounts.put("Shipped", 0);
            statusCounts.put("Pending", 0);
            statusCounts.put("Cancelled", 0);
            statusCounts.put("Return Received", 0);
            statusCounts.put("Refunded", 0); // Will be calculated

            int returnReceivedCount = 0;
            for (List<Object> row : fulfillmentData) {
                String status = row.get(0).toString();
                int count = parseInteger(row.get(1));
                statusCounts.put(status, count);
                if ("Return Received".equals(status)) {
                    returnReceivedCount = count;
                }
            }

            // Refunded = Return Received / 3
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
