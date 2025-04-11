package com.databin.api.controller;

import com.databin.api.service.StarTreeService;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

@RestController
@RequestMapping("/api/inventory")
@CrossOrigin(origins = "http://localhost:5173")
public class InventoryMetricsController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Inventory Turnover Rate & Low Stock Alerts (with date range)
    @GetMapping("/turnover-and-alerts")
    public ResponseEntity<?> getInventoryTurnoverAndLowStock(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate,
            @RequestParam(name = "threshold", defaultValue = "10") int threshold) {
        try {
            // 🔹 Inventory Turnover (monthly basis)
            String turnoverQuery = String.format("""
                WITH monthly_sales AS (
                    SELECT 
                        DATE_TRUNC('month', o.order_date) AS month,
                        SUM(o.quantity) AS total_sold
                    FROM orders o
                    WHERE o.order_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
                    GROUP BY month
                ),
                monthly_inventory AS (
                    SELECT 
                        DATE_TRUNC('month', i.restock_date) AS month,
                        AVG(i.stock_quantity) AS avg_stock
                    FROM inventory i
                    WHERE i.restock_date BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'
                    GROUP BY month
                )
                SELECT 
                    COALESCE(ms.month, mi.month) AS month,
                    COALESCE(ms.total_sold, 0) / NULLIF(mi.avg_stock, 0) AS turnover_rate
                FROM monthly_sales ms
                FULL OUTER JOIN monthly_inventory mi ON ms.month = mi.month
                ORDER BY month
            """, startDate, endDate, startDate, endDate);

            List<List<Object>> turnoverData = starTreeService.executeSqlQuery(turnoverQuery);

            List<Map<String, Object>> turnoverList = new ArrayList<>();
            for (List<Object> row : turnoverData) {
                String month = formatMonth(row.get(0));
                double rate = parseDouble(row.get(1));
                turnoverList.add(Map.of(
                        "month", month,
                        "turnover_rate", rate
                ));
            }

            // 🔹 Low Stock Products (Top 3 with restock date, no timestamp)
            String lowStockQuery = """
                SELECT 
                    p.name AS product_name,
                    i.stock_quantity,
                    i.restock_date
                FROM inventory i
                JOIN products p ON i.product_id = p.id
                WHERE i.status = 'Low Stock'
                ORDER BY i.stock_quantity ASC
                LIMIT 3
            """;

            List<List<Object>> lowStockData = starTreeService.executeSqlQuery(lowStockQuery);
            List<Map<String, Object>> lowStockList = new ArrayList<>();
            for (List<Object> row : lowStockData) {
                String name = Objects.toString(row.get(0), "Unknown");
                int qty = parseInteger(row.get(1));
                String restockDate = formatDate(row.get(2)); // ⬅️ Format date to exclude timestamp
                lowStockList.add(Map.of(
                        "product_name", name,
                        "stock_quantity", qty,
                        "restock_date", restockDate
                ));
            }

            return ResponseEntity.ok(Map.of(
                    "turnover_rates", turnoverList,
                    "low_stock_alerts", lowStockList
            ));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch inventory metrics"));
        }
    }

    // 🔧 Format month (for turnover)
    private String formatMonth(Object obj) {
        if (obj == null) return "N/A";

        if (obj instanceof java.sql.Timestamp ts) {
            return ts.toLocalDateTime().getMonth().toString() + " " + ts.toLocalDateTime().getYear();
        }

        if (obj instanceof Number epoch) {
            Instant instant = Instant.ofEpochMilli(epoch.longValue());
            LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return date.getMonth().toString() + " " + date.getYear();
        }

        if (obj instanceof String str) {
            try {
                long millis = Long.parseLong(str);
                Instant instant = Instant.ofEpochMilli(millis);
                LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
                return date.getMonth().toString() + " " + date.getYear();
            } catch (NumberFormatException ignored) {}
        }

        return obj.toString();
    }

    // 🔧 Format restock date (yyyy-MM-dd)
    private String formatDate(Object obj) {
        if (obj == null) return "N/A";

        if (obj instanceof java.sql.Timestamp ts) {
            return ts.toLocalDateTime().toLocalDate().toString(); // Only date part
        }

        if (obj instanceof Number epoch) {
            Instant instant = Instant.ofEpochMilli(epoch.longValue());
            LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return date.toString();
        }

        if (obj instanceof String str) {
            try {
                long millis = Long.parseLong(str);
                Instant instant = Instant.ofEpochMilli(millis);
                LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
                return date.toString();
            } catch (NumberFormatException ignored) {}
        }

        return obj.toString();
    }

    // 🔧 Convert Object to Integer
    private int parseInteger(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return ((Number) obj).intValue();
        try {
            return Integer.parseInt(obj.toString());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer format: " + obj, e);
        }
    }

    // 🔧 Convert Object to Double
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
