package com.epam.reportportal.migration.steps.logs;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
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
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public Step migrateLogStep() {
		prepareCollectionForReading();
		return stepBuilderFactory.get("log")
				.partitioner("slaveLogStep", logPartitioner())
				.gridSize((testItemRefs.size() / 1_00) == 0 ? 1 : testItemRefs.size() / 1_00)
				.step(slaveLogStep())
				.taskExecutor(threadPoolTaskExecutor)
				.listener(chunkCountListener)
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
	public LogPartitioner logPartitioner() {
		LogPartitioner logPartitioner = new LogPartitioner();
		logPartitioner.setItemIds(testItemRefs);
		return logPartitioner;
	}

	@Bean
	@StepScope
	public MongoItemReader<DBObject> logItemReader(@Value("#{stepExecutionContext[minValue]}") Integer minIndex,
			@Value("#{stepExecutionContext[maxValue]}") Integer maxIndex) {
		MongoItemReader<DBObject> itemReader = new MongoItemReader<>();
		itemReader.setTemplate(mongoTemplate);
		itemReader.setTargetType(DBObject.class);
		itemReader.setPageSize(batchSize);
		itemReader.setCollection("log");
		itemReader.setSort(new HashMap<>() {{
			put("logTime", Sort.Direction.ASC);
		}});
		final LinkedList<Object> objects = new LinkedList<>();
		objects.add(new Object());
		objects.add(new Object());
		objects.add(new Object());
		objects.set(1, testItemRefs.subList(minIndex, maxIndex));
		objects.set(2, Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant()));
		itemReader.setParameterValues(objects);
		itemReader.setQuery("{$and : [ {testItemRef : {$in : ?1}}, {logTime : {$gte : ?2}} ] }");
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
		query.with(new Sort(Sort.Direction.ASC, "_id"));
		query.fields().include("_id");
		testItemRefs = mongoTemplate.find(query, DBObject.class, OPTIMIZED_TEST_COLLECTION)
				.stream()
				.map(it -> it.get("_id"))
				.map(Object::toString)
				.collect(Collectors.toList());
	}
}
