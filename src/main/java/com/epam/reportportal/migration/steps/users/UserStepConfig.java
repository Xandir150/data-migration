package com.epam.reportportal.migration.steps.users;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.LocalDate;
import java.time.ZoneOffset;
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

	public MongoItemReader<DBObject> userMongoItemReader(String projectNm) {
		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());
		DBObject project = mongoTemplate.findOne(Query.query(Criteria.where("_id").is(projectNm)), DBObject.class, "project");
		Object[] users = ((BasicDBList) project.get("users")).stream().map(it -> ((DBObject) it).get("login")).toArray();
		MongoItemReader<DBObject> user = MigrationUtils.getMongoItemReader(mongoTemplate, "user");
		user.setQuery("{ $and : [ {'metaInfo.lastLogin' : {$gte : ?0}}, { _id : {$in : ?1}} ]}");
		user.setParameterValues(Lists.newArrayList(fromDate, users));
		user.setPageSize(CHUNK_SIZE);
		return user;
	}

	@Bean
	@Scope(value = "prototype")
	public Step migrateUsersStep(String projectNm) {
		return stepBuilderFactory.get("user").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(userMongoItemReader(projectNm))
				.processor(item -> item)
				.writer(userWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

}
