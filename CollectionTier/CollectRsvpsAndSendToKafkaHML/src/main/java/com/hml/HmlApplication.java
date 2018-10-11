package com.hml;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableReactiveMongoRepositories
public class HmlApplication {

    public static void main(String[] args) {
        SpringApplication.run(HmlApplication.class, args);
    }

    @Bean
    public ApplicationRunner initializeConnection(HmlHandler hmlHandler,
            RsvpsWebSocketHandler rsvpsWebSocketHandler) {
        return args -> {
          hmlHandler.recoverAfterRestart();
          rsvpsWebSocketHandler.doHandshake();          
        };                
    }
   
}
