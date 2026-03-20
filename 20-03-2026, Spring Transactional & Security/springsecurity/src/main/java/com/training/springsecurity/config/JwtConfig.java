package com.training.springsecurity.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JwtConfig {

    @Bean
    public String jwtSecret() {
        return "c3VwZXItc2VjdXJlLXNlY3JldC1rZXktdGhhdC1pcy1sb25nLWVub3VnaA==";// 256-bit recommended
    }
}