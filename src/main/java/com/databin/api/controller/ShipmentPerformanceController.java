package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/api/shipment-performance")
@CrossOrigin(origins = "http://localhost:5173")
public class ShipmentPerformanceController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Shipment Performance Data (with date range filter)
    @GetMapping
    public ResponseEntity<?> getShipmentPerformance(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate) {
        try {
        	String query = String.format("""
        		    WITH shipment_counts AS (
        		        SELECT 
        		            carrier,
        		            shipping_method,
        		            COUNT(*) AS total_shipments
        		        FROM shipment
        		        WHERE actual_delivery_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
        		        GROUP BY carrier, shipping_method
        		    )
        		    SELECT 
        		        carrier,
        		        SUM(CASE WHEN shipping_method = 'Standard' THEN total_shipments ELSE 0 END) AS standard_shipments,
        		        SUM(CASE WHEN shipping_method = 'Expedited' THEN total_shipments ELSE 0 END) AS expedited_shipments,
        		        SUM(CASE WHEN shipping_method = 'Same-Day' THEN total_shipments ELSE 0 END) AS same_day_shipments
        		    FROM shipment_counts
        		    GROUP BY carrier
        		    ORDER BY carrier
        		""", startDate, endDate);


            List<List<Object>> data = starTreeService.executeSqlQuery(query);
            List<Map<String, Object>> shipments = new ArrayList<>();

            for (List<Object> row : data) {
                shipments.add(Map.of(
                    "carrier", Objects.toString(row.get(0), "Unknown"),
                    "standard", parseInteger(row.get(1)),
                    "expedited", parseInteger(row.get(2)),
                    "same_day", parseInteger(row.get(3))
                ));
            }

            return ResponseEntity.ok(Map.of("shipment_performance", shipments));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch shipment performance data"));
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
