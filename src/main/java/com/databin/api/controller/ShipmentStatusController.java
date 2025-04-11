package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/api/shipment-status")
@CrossOrigin(origins = "http://localhost:5173")
public class ShipmentStatusController {

    @Autowired
    private StarTreeService starTreeService;

    @GetMapping("/count")
    public ResponseEntity<?> getOrderStatusCount(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate) {
        try {
            // ⚡ Flat, index-friendly query - very fast
            String fastQuery = String.format("""
                SELECT 'Delivered' AS status, COUNT(*) AS count
                FROM shipment
                WHERE shipment_status = 'Delivered'
                  AND actual_delivery_date >= TIMESTAMP '%s'
                  AND actual_delivery_date <= TIMESTAMP '%s'
                
                UNION ALL
                
                SELECT event_type AS status, COUNT(*) AS count
                FROM fulfillment_event
                WHERE event_type IN ('Shipped', 'Pending', 'Cancelled', 'Return Received')
                  AND event_time >= TIMESTAMP '%s'
                  AND event_time <= TIMESTAMP '%s'
                GROUP BY event_type
            """, startDate, endDate, startDate, endDate);

            List<List<Object>> result = starTreeService.executeSqlQuery(fastQuery);

            Map<String, Integer> statusCounts = new LinkedHashMap<>(Map.of(
                "Delivered", 0,
                "Shipped", 0,
                "Pending", 0,
                "Cancelled", 0,
                "Return Received", 0,
                "Refunded", 0
            ));

            int returnReceivedCount = 0;

            for (List<Object> row : result) {
                String status = row.get(0).toString();
                int count = parseInteger(row.get(1));
                statusCounts.put(status, count);
                if ("Return Received".equals(status)) {
                    returnReceivedCount = count;
                }
            }

            statusCounts.put("Refunded", Math.round(returnReceivedCount / 3.0f));
            return ResponseEntity.ok(statusCounts);

        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch shipment status counts"));
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
}
