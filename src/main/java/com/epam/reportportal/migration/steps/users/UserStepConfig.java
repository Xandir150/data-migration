package com.epam.reportportal.migration.steps.users;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class UserStepConfig {

	private static final int CHUNK_SIZE = 100;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Value("${rp.user.keepFrom}")
	private String keepFrom;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private ItemWriter<DBObject> userWriter;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	private TaskExecutor threadPoolTaskExecutor;

	@Bean
	public MongoItemReader<DBObject> userMongoItemReader() {
		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());
		MongoItemReader<DBObject> user = MigrationUtils.getMongoItemReader(mongoTemplate, "user");
		user.setQuery("{'metaInfo.lastLogin' : {$gte : ?0}})");
		user.setParameterValues(Collections.singletonList(fromDate));
		user.setPageSize(CHUNK_SIZE);
		return user;
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> userItemProcessor() {
		return item -> item;
	}

	@Bean
	public Step migrateUsersStep() {
		return stepBuilderFactory.get("user").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(userMongoItemReader())
				.processor(userItemProcessor())
				.writer(userWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

}
