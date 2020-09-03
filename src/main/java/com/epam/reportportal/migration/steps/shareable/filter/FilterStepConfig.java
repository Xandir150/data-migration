package com.epam.reportportal.migration.steps.shareable.filter;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.google.common.collect.Lists;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class FilterStepConfig {

	private static final int CHUNK_SIZE = 1000;

	public static Long ACL_CLASS;

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
	private ItemProcessor<DBObject, DBObject> filterProcessor;

	@Autowired
	private ItemWriter<DBObject> filterWriter;

	public MongoItemReader<DBObject> filterItemReader(String projectName) {
		executeInitialQueries();
		MongoItemReader<DBObject> reader = MigrationUtils.getMongoItemReader(mongoTemplate, "userFilter");
		reader.setQuery("{projectName : ?0}");
		reader.setParameterValues(Lists.newArrayList(projectName));
		reader.setPageSize(CHUNK_SIZE);
		return reader;
	}

	private void executeInitialQueries() {
		if (mongoTemplate.collectionExists("filterMapping")) {
			mongoTemplate.dropCollection("filterMapping");
		}
		try {
			ACL_CLASS = jdbcTemplate.queryForObject(
					"INSERT INTO acl_class (class, class_id_type) VALUES ('com.epam.ta.reportportal.entity.filter.UserFilter','java.lang.Long') RETURNING id",
					Long.class
			);
		} catch (Exception e) {
			ACL_CLASS = jdbcTemplate.queryForObject("SELECT id FROM acl_class WHERE class = 'com.epam.ta.reportportal.entity.filter.UserFilter'",
					Long.class
			);
		}
	}

	@Bean("migrateFilterStep")
	@Scope(value = "prototype")
	public Step migrateFilterStep(String projectName) {
		return stepBuilderFactory.get("filter").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(filterItemReader(projectName))
				.processor(filterProcessor)
				.writer(filterWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

}
