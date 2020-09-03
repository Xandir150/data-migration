package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.CacheableDataService;
import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.Collections;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class LaunchNumberConfig {

	private static final int CHUNK_SIZE = 1000;

	private static final String SELECT_LAUNCH_EXISTS = "SELECT exists(SELECT 1 FROM launch WHERE name=:nm)";

	private static final String INSERT_LAUNCH_NUMBER = "INSERT INTO launch_number (project_id, launch_name, number) VALUES (:pr, :ln, :num) ON CONFLICT DO NOTHING ";

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private ChunkListener chunkListener;

	@Autowired
	private TaskExecutor threadPoolTaskExecutor;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Bean(name = "migrateLaunchNumberStep")
	@Scope(value = "prototype")
	public Step migrateLaunchNumberStep(String projectName) {
		return stepBuilderFactory.get("launchNumber").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(launchNumberReader(projectName))
				.processor(launchNumberProcessor())
				.writer(launchNumberWriter())
				.taskExecutor(threadPoolTaskExecutor)
				.listener(chunkListener)
				.build();
	}

	public ItemReader<DBObject> launchNumberReader(String projectName) {
		MongoItemReader<DBObject> launchMetaInfo = MigrationUtils.getMongoItemReader(mongoTemplate, "launchMetaInfo");
		launchMetaInfo.setQuery("{?0 : {$exists : true}}");
		launchMetaInfo.setParameterValues(Collections.singletonList("projects." + projectName));
		launchMetaInfo.setPageSize(CHUNK_SIZE);
		return launchMetaInfo;
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> launchNumberProcessor() {
		return it -> {
			DBObject projects = (DBObject) it.get("projects");
			if (null == projects) {
				return null;
			}
			Boolean launchExists = jdbcTemplate.queryForObject(SELECT_LAUNCH_EXISTS,
					Collections.singletonMap("nm", it.get("_id").toString()),
					Boolean.class
			);
			if (!launchExists) {
				return null;
			}
			it.put("projectIds", new BasicDBObject());
			projects.toMap().entrySet().forEach(project -> updateDbObject(it, (Map.Entry) project));
			return it;
		};
	}

	@Bean
	public ItemWriter<DBObject> launchNumberWriter() {
		return it -> {
			SqlParameterSource[] sqlParameterSources = it.stream().flatMap(launchNumberItem -> {
				BasicDBObject projectIds = (BasicDBObject) launchNumberItem.get("projectIds");
				return projectIds.entrySet().stream().map(entry -> {
					MapSqlParameterSource parameterSource = new MapSqlParameterSource();
					parameterSource.addValue("pr", Long.valueOf(entry.getKey()));
					parameterSource.addValue("ln", launchNumberItem.get("_id").toString());
					parameterSource.addValue("num", entry.getValue());
					return parameterSource;
				}).collect(toList()).stream();
			}).toArray(SqlParameterSource[]::new);
			jdbcTemplate.batchUpdate(INSERT_LAUNCH_NUMBER, sqlParameterSources);
		};
	}

	private void updateDbObject(DBObject item, Map.Entry project) {
		if (project != null) {
			String projectName = (String) project.getKey();
			Long projectId = cacheableDataService.retrieveProjectId(projectName);
			if (projectId != null) {
				DBObject projectIds = (DBObject) item.get("projectIds");
				projectIds.put(String.valueOf(projectId), project.getValue());
			}
		}
	}
}
