package com.epam.reportportal.migration.steps.shareable.dashboard;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.google.common.collect.Lists;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class DashboardStepConfig {

	private static final int CHUNK_SIZE = 100;

	static Long ACL_CLASS;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private ChunkListener chunkCountListener;

	@Autowired
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private ItemProcessor<DBObject, DBObject> dashboardProcessor;

	@Autowired
	private ItemWriter<DBObject> dashboardWriter;

	@Value("${rp.project}")
	private String projectName;

	@PostConstruct
	public void initialQueries() {
		try {
			ACL_CLASS = jdbcTemplate.queryForObject(
					"INSERT INTO acl_class (class, class_id_type) VALUES ('com.epam.ta.reportportal.entity.dashboard.Dashboard','java.lang.Long') RETURNING id",
					Long.class
			);
		} catch (Exception e) {
			ACL_CLASS = jdbcTemplate.queryForObject("SELECT id FROM acl_class WHERE class = 'com.epam.ta.reportportal.entity.dashboard.Dashboard'",
					Long.class
			);
		}
	}

	@Bean
	public MongoItemReader<DBObject> dashboardItemReader() {
		MongoItemReader<DBObject> reader = MigrationUtils.getMongoItemReader(mongoTemplate, "dashboard");
		reader.setQuery("{projectName : ?0}");
		reader.setParameterValues(Lists.newArrayList(projectName));
		reader.setPageSize(CHUNK_SIZE);
		return reader;
	}

	@Bean("migrateDashboardStep")
	public Step migrateDashboardStep() {
		return stepBuilderFactory.get("dashboard").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(dashboardItemReader())
				.processor(dashboardProcessor)
				.writer(dashboardWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}
}
