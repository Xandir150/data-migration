package com.epam.reportportal.migration.steps.logs;

import com.epam.reportportal.migration.seek.MongoSeekItemReader;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.items.ItemsStepConfig.OPTIMIZED_TEST_COLLECTION;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class LogStepConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Value("${rp.items.batch}")
	private int batchSize;

	@Value("${rp.grid.size}")
	private int gridSize;

	@Value("${rp.log.keepFrom}")
	private String keepFrom;

	@Value("${rp.pool.corePoolSize}")
	private int corePoolSize;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("logProcessor")
	private ItemProcessor<DBObject, DBObject> logProcessor;

	@Autowired
	@Qualifier("logWriter")
	private ItemWriter<DBObject> logWriter;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	private TaskExecutor threadPoolTaskExecutor;

	private List<String> testItemRefs;

	@Bean(name = "migrateLogStep")
	public Step migrateLogStep() {
		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());
		prepareCollectionForReading();
		return stepBuilderFactory.get("log")
				.partitioner("slaveLogStep", logPartitioner(findStartObject(fromDate), findLastObject(fromDate)))
				.gridSize(gridSize)
				.step(slaveLogStep())
				.taskExecutor(threadPoolTaskExecutor)
				.listener(chunkCountListener)
//				.allowStartIfComplete(true)
				.build();
	}

	@Bean
	public Step slaveLogStep() {
		return stepBuilderFactory.get("slaveLogStep").<DBObject, DBObject>chunk(batchSize).reader(logItemReader(null, null))
				.processor(logProcessor)
				.writer(logWriter)
				.build();
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public com.epam.reportportal.migration.steps.items.DatePartitioner logPartitioner(DBObject minObject, DBObject maxObject) {
		final com.epam.reportportal.migration.steps.items.DatePartitioner partitioner = new com.epam.reportportal.migration.steps.items.DatePartitioner();
		if (minObject == null || maxObject == null) {
			return partitioner;
		}
		partitioner.setMinDate((Date) minObject.get("logTime"));
		partitioner.setMaxDate((Date) maxObject.get("logTime"));
		return partitioner;
	}

	@Bean
	@StepScope
	public MongoSeekItemReader<DBObject> logItemReader(@Value("#{stepExecutionContext[minValue]}") Long minTime,
			@Value("#{stepExecutionContext[maxValue]}") Long maxTime) {
		MongoSeekItemReader<DBObject> itemReader = new MongoSeekItemReader<>();
		itemReader.setTemplate(mongoTemplate);
		itemReader.setTargetType(DBObject.class);
		itemReader.setCollection("log");
		itemReader.setSort(new HashMap<>() {{
			put("logTime", Sort.Direction.ASC);
		}});
		itemReader.setLimit(batchSize);
		itemReader.setDateField("logTime");
		itemReader.setCurrentDate(new Date(minTime));
		itemReader.setLatestDate(new Date(maxTime));
		final LinkedList<Object> objects = new LinkedList<>();
		objects.add(new Object());
		objects.add(new Object());
		objects.add(new Object());
		objects.set(0, itemReader.getCurrentDate());
		objects.set(2, testItemRefs.toArray());
		itemReader.setParameterValues(objects);
		itemReader.setQuery("{$and : [ {logTime : {$gte : ?1}}, {testItemRef : {$in : ?2}}] }");
		return itemReader;
	}

	private void prepareCollectionForReading() {
		if (mongoTemplate.getCollection("log")
				.getIndexInfo()
				.stream()
				.noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("logTime"))) {
			LOGGER.info("Adding 'log_time' index to log collection");
			mongoTemplate.indexOps("log").ensureIndex(new Index("logTime", Sort.Direction.ASC).named("logTime"));
			LOGGER.info("Adding 'log_time' index to log collection successfully finished");
		}
		Query query = new Query();
		query.fields().include("_id");
		testItemRefs = mongoTemplate.find(query, DBObject.class, OPTIMIZED_TEST_COLLECTION)
				.stream()
				.map(it -> it.get("_id"))
				.map(Object::toString)
				.collect(Collectors.toList());
	}

	private DBObject findStartObject(Date fromDate) {
		Query query = Query.query(Criteria.where("logTime").gte(fromDate).and("testItemRef").in(testItemRefs))
				.with(new Sort(Sort.Direction.ASC, "logTime"))
				.limit(1);
		return mongoTemplate.findOne(query, DBObject.class, "log");
	}

	private DBObject findLastObject(Date fromDate) {
		Query query = Query.query(Criteria.where("logTime").gte(fromDate).and("testItemRef").in(testItemRefs))
				.with(new Sort(Sort.Direction.DESC, "logTime"))
				.limit(1);
		return mongoTemplate.findOne(query, DBObject.class, "log");
	}
}
