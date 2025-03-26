package com.databin.api.controller;

import com.databin.api.dto.SqlRequest;
import com.databin.api.service.StarTreeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/star-tree")
public class StarTreeController {

    private final StarTreeService starTreeService;

    @Autowired
    public StarTreeController(StarTreeService starTreeService) {
        this.starTreeService = starTreeService;
    }

    @PostMapping("/query")
    public String query(@RequestBody SqlRequest request) throws IOException {
        //System.out.println("Received SQL Query: " + request.getSql());
        return starTreeService.executeSqlQuery(request.getSql());
    }
}
