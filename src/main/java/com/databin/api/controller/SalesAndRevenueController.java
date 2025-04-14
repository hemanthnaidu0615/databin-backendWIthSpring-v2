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
@RequestMapping("/api/sales-revenue")
@CrossOrigin(origins = "http://localhost:5173")
public class SalesAndRevenueController {

    @Autowired
    private StarTreeService starTreeService;

    // 📌 API: Get Total Sales Data (with date filter)
    @GetMapping("/sales-data")
    public ResponseEntity<?> getSalesData(
            @RequestParam("startDate") String startDate,
            @RequestParam("endDate") String endDate) {
        try {
            String query = "SELECT SUM(total_amount) AS total_sales FROM orders " +
                    "WHERE order_date BETWEEN TIMESTAMP '" + startDate + "' AND TIMESTAMP '" + endDate + "'";

            List<List<Object>> data = starTreeService.executeSqlQuery(query);
            double totalSales = data.isEmpty() || data.get(0).get(0) == null ? 0.0 : parseDouble(data.get(0).get(0));

            return ResponseEntity.ok(Map.of("total_sales", totalSales));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch total sales data"));
        }
    }

    // 📌 API: Get Revenue Trends Over Time (with date filter)
    @GetMapping("/revenue-trends")
    public ResponseEntity<?> getRevenueTrends(
            @RequestParam("startDate") String startDate,
            @RequestParam("endDate") String endDate) {
        try {
            String query =
                    "SELECT " +
                    "DATE_TRUNC('month', order_date) AS month, " +
                    "SUM(subtotal) AS monthly_revenue " +
                    "FROM orders " +
                    "WHERE order_date BETWEEN TIMESTAMP '" + startDate + "' AND TIMESTAMP '" + endDate + "' " +
                    "GROUP BY month " +
                    "ORDER BY month ASC";

            List<List<Object>> data = starTreeService.executeSqlQuery(query);
            return ResponseEntity.ok(Map.of("revenue_trends", data));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch revenue trends"));
        }
    }

    // 📌 API: Get Forecasted Sales (SMA based on selected range)
    @GetMapping("/forecasted-sales")
    public ResponseEntity<?> getForecastedSales(
            @RequestParam("startDate") String startDate,
            @RequestParam("endDate") String endDate) {
        try {
            String query =
                    "SELECT " +
                    "DATE_TRUNC('month', order_date) AS month, " +
                    "SUM(total_amount) AS monthly_sales " +
                    "FROM orders " +
                    "WHERE order_date BETWEEN TIMESTAMP '" + startDate + "' AND TIMESTAMP '" + endDate + "' " +
                    "GROUP BY month " +
                    "ORDER BY month ASC";

            List<List<Object>> data = starTreeService.executeSqlQuery(query);

            double forecastedSales = 0.0;
            if (!data.isEmpty()) {
                double total = 0.0;
                for (List<Object> row : data) {
                    total += parseDouble(row.get(1));
                }
                forecastedSales = total / data.size(); // Simple Moving Average
            }

            return ResponseEntity.ok(Map.of("forecasted_sales", forecastedSales));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to fetch forecasted sales"));
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
