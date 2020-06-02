package com.epam.reportportal.migration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class DataMigrationApplication {

	@Value("${rp.pool.corePoolSize}")
	private int corePoolSize;

	@Value("${rp.pool.maxPoolSize}")
	private int maxPoolSize;

	@Bean
	public Cache<String, Long> customStatisticsFieldsCache() {
		return Caffeine.newBuilder().initialCapacity(100).maximumSize(1000).expireAfterAccess(30, TimeUnit.HOURS).build();
	}

	@Bean
	public Cache<String, Long> locatorsFieldsCache() {
		return Caffeine.newBuilder().initialCapacity(100).maximumSize(1000).expireAfterAccess(30, TimeUnit.HOURS).build();
	}

	@Bean
	// mongo uuid -> postgres id
	public Cache<String, Object> idsCache() {
		return Caffeine.newBuilder().softValues().build();
	}

	@Bean
	// mongo userName -> postgres user id
	public Cache<String, Long> usersCache() {
		return Caffeine.newBuilder().initialCapacity(5_000).maximumSize(10_000).expireAfterAccess(30, TimeUnit.HOURS).build();
	}

	@Bean(name = "statisticsFields")
	// Default statistics fields have fixed ids in PostgreSQL
	public Map<String, Long> statisticsFields() {
		Map<String, Long> statisticsFields = new HashMap<>(14);
		statisticsFields.put("total", 1L);
		statisticsFields.put("passed", 2L);
		statisticsFields.put("skipped", 3L);
		statisticsFields.put("failed", 4L);
		statisticsFields.put("statistics$defects$automation_bug$total", 5L);
		statisticsFields.put("statistics$defects$automation_bug$ab001", 6L);
		statisticsFields.put("statistics$defects$product_bug$total", 7L);
		statisticsFields.put("statistics$defects$product_bug$pb001", 8L);
		statisticsFields.put("statistics$defects$system_issue$total", 9L);
		statisticsFields.put("statistics$defects$system_issue$si001", 10L);
		statisticsFields.put("statistics$defects$to_investigate$total", 11L);
		statisticsFields.put("statistics$defects$to_investigate$ti001", 12L);
		statisticsFields.put("statistics$defects$no_defect$total", 13L);
		statisticsFields.put("statistics$defects$no_defect$nd001", 14L);
		return statisticsFields;
	}

	@Bean("threadPoolTaskExecutor")
	public ThreadPoolTaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
		threadPoolTaskExecutor.setMaxPoolSize(maxPoolSize);
		return threadPoolTaskExecutor;
	}

	@Bean
	@Primary
	public PlatformTransactionManager transactionManager(@Autowired DataSource dataSource) {
		return new DataSourceTransactionManager(dataSource);
	}

	public static void main(String[] args) {
		SpringApplication.run(DataMigrationApplication.class, args);
	}
}
