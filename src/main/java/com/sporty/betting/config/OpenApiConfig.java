package com.sporty.betting.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Bet Settlement Trigger Service API")
                        .version("1.0.0")
                        .description("API for publishing sports event outcomes and triggering bet settlement via Kafka and RocketMQ"));
    }
}
