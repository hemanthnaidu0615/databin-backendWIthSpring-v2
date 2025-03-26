package com.databin.api.service;

import com.databin.api.config.*;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class StarTreeService {

    private final StarTreeConfig config;

    @Autowired
    public StarTreeService(StarTreeConfig config) {
        this.config = config;
    }

    public String executeSqlQuery(String sql) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(config.getBrokerUrl());

            // Set Headers
//            post.setHeader("Authorization", "Bearer st-QLeDDSEzI6zRl75t-fn1YwdP7it6XusWJlLYL1uoVnXt7ldXV");
//            post.setHeader("Content-Type", "application/json");
//            System.out.println("Final Auth Header: Bearer " + config.getAuthToken().trim());
//
//            
           // System.out.println("Broker URL: " + config.getBrokerUrl());
           //.out.println("Auth Token: " + config.getAuthToken());
            
            post.setHeader("Authorization", "Bearer " + config.getAuthToken());
            post.setHeader("Content-Type", "application/json");
            post.setHeader("Accept", "application/json");
            post.setHeader("database", "ws_2tcna4wdsaga");


            // Create request body
            String jsonBody = "{\"sql\":\"" + sql + "\"}";
            HttpEntity entity = new StringEntity(jsonBody, ContentType.APPLICATION_JSON);
            post.setEntity(entity);
           // System.out.println("Request Body: " + jsonBody);

            // Execute request using a response handler (Prevents deprecation issues)
            return client.execute(post, response -> {
                if (response.getEntity() == null) {
                    return "No Response";
                }
                return new String(response.getEntity().getContent().readAllBytes());
            });
        }}}
    
