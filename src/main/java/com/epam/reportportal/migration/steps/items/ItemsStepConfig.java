package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.seek.MongoSeekItemReader;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
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
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class ItemsStepConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	public static String OPTIMIZED_TEST_COLLECTION = "optimizeTest";

	public static AtomicLong TEST_ITEM_ID = new AtomicLong(1);


	@Value("${rp.items.batch}")
	private int batchSize;

	@Value("${rp.grid.size}")
	private int gridSize;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	@Qualifier("testItemProcessor")
	private ItemProcessor testItemProcessor;

	@Autowired
	@Qualifier("testItemWriter")
	private ItemWriter testItemWriter;

	@Autowired
	@Qualifier("threadPoolTaskExecutor")
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;

	@Value("${rp.test.keepFrom}")
	private String keepFrom;

	@Bean
	public TriFunction<Integer, DBObject, DBObject, Step> itemStepFactory() {
		return this::migrateItemStep;
	}

	@Bean
	public List<Step> levelItemsFlow() {

		prepareCollectionForMigration();

		DBObject testItem = mongoTemplate.findOne(new Query().with(new Sort(Sort.Direction.DESC, "pathLevel")).limit(1),
				DBObject.class,
				OPTIMIZED_TEST_COLLECTION
		);
		int pathSize = (int) testItem.get("pathLevel");
		List<Step> steps = new LinkedList<>();
		for (int i = 0; i <= pathSize; i++) {
			Step step = itemStepFactory().apply(i, findStartObject(i), findLastObject(i));
			steps.add(step);
		}
		return steps;
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public Step migrateItemStep(int i, DBObject minObject, DBObject maxObject) {
		return stepBuilderFactory.get("item." + i)
				.partitioner("slaveItemStep." + i, partitioner(i, minObject, maxObject))
				.gridSize(gridSize)
				.step(slaveItemStep(i))
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public DatePartitioner partitioner(Integer i, DBObject minObject, DBObject maxObject) {
		DatePartitioner partitioner = new DatePartitioner();
		partitioner.setPathLevel(i);
		partitioner.setMinDate((Date) minObject.get("start_time"));
		partitioner.setMaxDate((Date) maxObject.get("start_time"));
		return partitioner;
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public Step slaveItemStep(int i) {
		return stepBuilderFactory.get("slaveItemStep." + i).<DBObject, DBObject>chunk(batchSize).reader(testItemReader(null, null, null))
				.processor(testItemProcessor)
				.writer(testItemWriter)
				.listener(chunkCountListener)
				.build();
	}

	@Bean
	@StepScope
	public MongoSeekItemReader<DBObject> testItemReader(@Value("#{stepExecutionContext[minValue]}") Long minTime,
			@Value("#{stepExecutionContext[maxValue]}") Long maxTime, @Value("#{stepExecutionContext[pathLevel]}") Integer pathLevel) {
		MongoSeekItemReader<DBObject> itemReader = new MongoSeekItemReader<>();
		itemReader.setTemplate(mongoTemplate);
		itemReader.setTargetType(DBObject.class);
		itemReader.setCollection(ItemsStepConfig.OPTIMIZED_TEST_COLLECTION);
		itemReader.setSort(new HashMap<String, Sort.Direction>() {{
			put("start_time", Sort.Direction.ASC);
		}});
		itemReader.setLimit(batchSize);
		itemReader.setDateField("start_time");
		itemReader.setCurrentDate(new Date(minTime));
		itemReader.setLatestDate(new Date(maxTime));
		itemReader.setQuery("{$and : [ {start_time : {$gte : ?1}}, {'pathLevel' : ?0}]}");
		itemReader.setParameterValues(Lists.newArrayList(pathLevel));
		return itemReader;
	}

	private DBObject findStartObject(Integer pathLevel) {
		Query query = Query.query(Criteria.where("pathLevel").is(pathLevel)).with(new Sort(Sort.Direction.ASC, "start_time")).limit(1);
		return mongoTemplate.findOne(query, DBObject.class, OPTIMIZED_TEST_COLLECTION);
	}

	private DBObject findLastObject(Integer pathLevel) {
		Query query = Query.query(Criteria.where("pathLevel").is(pathLevel)).with(new Sort(Sort.Direction.DESC, "start_time")).limit(1);
		return mongoTemplate.findOne(query, DBObject.class, OPTIMIZED_TEST_COLLECTION);
	}

	private void prepareCollectionForMigration() {
		prepareIndexTestItemStartTime();
		prepareOptimizedTestItemCollection();
		prepareIndexOptimizedPath();
	}

	private void prepareIndexOptimizedPath() {
		List<DBObject> indexInfoOptimized = mongoTemplate.getCollection(OPTIMIZED_TEST_COLLECTION).getIndexInfo();
		if (indexInfoOptimized.stream().noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("migration_index"))) {
			LOGGER.info("Adding 'migration_index' index to optimizedTest collection");
			mongoTemplate.indexOps(OPTIMIZED_TEST_COLLECTION)
					.ensureIndex(new CompoundIndexDefinition(new BasicDBObject("start_time", 1).append("pathLevel", 1)).named(
							"migration_index"));
			LOGGER.info("Adding 'migration_index' index to optimizedTest collection successfully finished");
		}
		if (indexInfoOptimized.stream().noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("pathLevel"))) {
			LOGGER.info("Adding 'pathLevel' index to optimizedTest collection");
			mongoTemplate.indexOps(OPTIMIZED_TEST_COLLECTION).ensureIndex(new Index("pathLevel", Sort.Direction.ASC).named("pathLevel"));
			LOGGER.info("Adding 'migration_index' index to optimizedTest collection successfully finished");
		}
	}

	private void prepareOptimizedTestItemCollection() {
		if (mongoTemplate.collectionExists(OPTIMIZED_TEST_COLLECTION)) {
			return;
		}

		mongoTemplate.createCollection(OPTIMIZED_TEST_COLLECTION);

		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());

		LOGGER.info("Adding 'pathLevel' field to optimizeTest collection");
		mongoTemplate.aggregate(Aggregation.newAggregation(Aggregation.match(Criteria.where("start_time").gte(fromDate)),
				context -> new BasicDBObject("$addFields", new BasicDBObject("pathLevel", new BasicDBObject("$size", "$path"))),
				Aggregation.out(OPTIMIZED_TEST_COLLECTION)
		), "testItem", Object.class);

		LOGGER.info("Adding 'pathLevel' field to testItem collection successfully finished");
	}

	private void prepareIndexTestItemStartTime() {
		List<DBObject> indexInfo = mongoTemplate.getCollection("testItem").getIndexInfo();
		if (indexInfo.stream().noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("start_time"))) {
			LOGGER.info("Adding 'start_time' index to testItem collection");
			mongoTemplate.indexOps("testItem").ensureIndex(new Index("start_time", Sort.Direction.ASC).named("start_time"));
			LOGGER.info("Adding 'start_time' index to testItem collection successfully finished");
		}
	}

}
