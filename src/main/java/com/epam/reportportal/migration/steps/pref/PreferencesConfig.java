package com.epam.reportportal.migration.steps.pref;

import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@SuppressWarnings("ALL")
@Configuration
public class PreferencesConfig {

	private static final int CHUNK_SIZE = 100;

	private static final String INSERT_USER_PREFERENCES = "INSERT INTO user_preference (project_id, user_id, filter_id) VALUES (?,?,?) ON CONFLICT DO NOTHING";

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Value("${rp.project}")
	private String projectName;

	@Bean
	public MongoItemReader<DBObject> preferencesReader() {
		MongoItemReader<DBObject> user = MigrationUtils.getMongoItemReader(mongoTemplate, "userPreference");
		user.setQuery("{projectRef : ?0}");
		user.setParameterValues(Lists.newArrayList(projectName));
		user.setPageSize(CHUNK_SIZE);
		return user;
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> preferencesProcessor() {
		return item -> {
			BasicDBObject launchTabs = (BasicDBObject) item.get("launchTabs");
			if (launchTabs == null) {
				return null;
			}
			if (CollectionUtils.isEmpty((BasicDBList) launchTabs.get("filters"))) {
				return null;
			}
			Long projectId = cacheableDataService.retrieveProjectId((String) item.get("projectRef"));
			Long userId = cacheableDataService.retrieveUser((String) item.get("userRef"));
			if (projectId != null && userId != null) {
				item.put("projectId", projectId);
				item.put("userId", userId);
				return item;
			}
			return null;
		};
	}

	@Bean
	public ItemWriter<? super DBObject> preferencesWriter() {
		return items -> {
			Set<ObjectId> mongoIds = items.stream()
					.map(it -> it.get("launchTabs"))
					.flatMap(it -> ((BasicDBList) ((DBObject) it).get("filters")).stream())
					.filter(Objects::nonNull)
					.map(String.class::cast)
					.map(ObjectId::new)
					.collect(Collectors.toSet());
			Map<String, Long> filterIdsMapping = cacheableDataService.loadFilterIdsMapping(mongoIds);
			List<Object[]> params = prepareSqlParams(items, filterIdsMapping);
			jdbcTemplate.batchUpdate(INSERT_USER_PREFERENCES, params);
		};
	}

	private List<Object[]> prepareSqlParams(List<? extends DBObject> items, Map<String, Long> filterIdsMapping) {
		return items.stream().flatMap(it -> {
			BasicDBList filters = (BasicDBList) ((DBObject) it.get("launchTabs")).get("filters");
			if (!CollectionUtils.isEmpty(filters)) {
				return filters.stream().filter(id -> filterIdsMapping.containsKey(id)).map(filterId -> {
					Object[] parms = { it.get("projectId"), it.get("userId"), filterIdsMapping.get(filterId) };
					return parms;
				}).collect(Collectors.toList()).stream();
			}
			return Stream.empty();
		}).collect(Collectors.toList());
	}

	@Bean
	public Step migratePreferencesStep() {
		return stepBuilderFactory.get("preferences").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(preferencesReader())
				.processor(preferencesProcessor())
				.writer(preferencesWriter())
				.listener(chunkCountListener)
				.build();
	}

}
