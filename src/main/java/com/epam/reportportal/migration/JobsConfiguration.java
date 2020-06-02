package com.epam.reportportal.migration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

/**
 * @author Pavel Bortnik
 */
@Configuration
@EnableBatchProcessing
public class JobsConfiguration {

	public static final String CACHE_MAPPING = "cacheMapping";

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	@Qualifier("migrateLaunchStep")
	private Step migrateLaunchStep;

	@Autowired
	@Qualifier("migrateLaunchNumberStep")
	private Step migrateLaunchNumberStep;

	@Autowired
	@Qualifier("levelItemsFlow")
	private List<Step> levelItemsFlow;

	@Autowired
	@Qualifier("migrateLogStep")
	private Step migrateLogStep;

	@Autowired
	private MigrationJobExecutionListener migrationJobExecutionListener;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Bean
	public Job job() {
		SimpleJobBuilder job = jobBuilderFactory.get("migrationDataJob")
				.listener(migrationJobExecutionListener)
				.start(migrateLaunchStep)
				.next(migrateLaunchNumberStep);
		for (Step s : levelItemsFlow) {
			job = job.next(s);
		}
		job.next(migrateLogStep);
		return job.build();
	}

}
