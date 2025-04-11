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

    @GetMapping("/turnover-and-alerts")
    public ResponseEntity<?> getInventoryTurnoverAndLowStock(
            @RequestParam(name = "startDate") String startDate,
            @RequestParam(name = "endDate") String endDate,
            @RequestParam(name = "threshold", defaultValue = "10") int threshold) {

        try {
            // ✅ Faster Turnover Query (INNER JOIN by month, drop FULL OUTER JOIN)
            StringBuilder turnoverQuery = new StringBuilder();
            turnoverQuery.append("WITH monthly_sales AS (")
                    .append("SELECT DATE_TRUNC('month', o.order_date) AS month, SUM(o.quantity) AS total_sold ")
                    .append("FROM orders o ")
                    .append("WHERE o.order_date BETWEEN TIMESTAMP '").append(startDate).append("' AND TIMESTAMP '").append(endDate).append("' ")
                    .append("GROUP BY 1), ")
                    .append("monthly_inventory AS (")
                    .append("SELECT DATE_TRUNC('month', i.restock_date) AS month, AVG(i.stock_quantity) AS avg_stock ")
                    .append("FROM inventory i ")
                    .append("WHERE i.restock_date BETWEEN TIMESTAMP '").append(startDate).append("' AND TIMESTAMP '").append(endDate).append("' ")
                    .append("GROUP BY 1) ")
                    .append("SELECT s.month, ROUND(s.total_sold / NULLIF(i.avg_stock, 0), 2) AS turnover_rate ")
                    .append("FROM monthly_sales s ")
                    .append("JOIN monthly_inventory i ON s.month = i.month ")
                    .append("ORDER BY s.month");

            List<List<Object>> turnoverData = starTreeService.executeSqlQuery(turnoverQuery.toString());

            List<Map<String, Object>> turnoverList = new ArrayList<>(turnoverData.size());
            for (List<Object> row : turnoverData) {
                turnoverList.add(Map.of(
                        "month", formatMonth(row.get(0)),
                        "turnover_rate", parseDouble(row.get(1))
                ));
            }

            // ✅ Low Stock Query (remove 'status' condition)
            StringBuilder lowStockQuery = new StringBuilder();
            lowStockQuery.append("SELECT p.name, i.stock_quantity, i.restock_date ")
                    .append("FROM inventory i ")
                    .append("JOIN products p ON i.product_id = p.id ")
                    .append("WHERE i.stock_quantity <= ").append(threshold).append(" ")
                    .append("ORDER BY i.stock_quantity ASC ")
                    .append("LIMIT 3");

            List<List<Object>> lowStockData = starTreeService.executeSqlQuery(lowStockQuery.toString());

            List<Map<String, Object>> lowStockList = new ArrayList<>(lowStockData.size());
            for (List<Object> row : lowStockData) {
                lowStockList.add(Map.of(
                        "product_name", Objects.toString(row.get(0), "Unknown"),
                        "stock_quantity", parseInteger(row.get(1)),
                        "restock_date", formatDate(row.get(2))
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

    private String formatDate(Object obj) {
        if (obj == null) return "N/A";

        if (obj instanceof java.sql.Timestamp ts) {
            return ts.toLocalDateTime().toLocalDate().toString();
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

    private int parseInteger(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return ((Number) obj).intValue();
        try {
            return Integer.parseInt(obj.toString());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer format: " + obj, e);
        }
    }

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
