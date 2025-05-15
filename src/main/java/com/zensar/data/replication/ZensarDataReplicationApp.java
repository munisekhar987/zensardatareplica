package com.zensar.data.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ZensarDataReplicationApp {

	public static void main(String[] args) {
		SpringApplication.run(ZensarDataReplicationApp.class, args);
	}

	/**
	 * ObjectMapper bean for JSON processing
	 */
	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper();
	}

}
