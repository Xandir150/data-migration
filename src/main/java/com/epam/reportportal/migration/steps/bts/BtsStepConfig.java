package com.epam.reportportal.migration.steps.bts;

import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class BtsStepConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final int CHUNK_SIZE = 200;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private BtsItemWriter btsItemWriter;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Value("${rp.project}")
	private String projectName;

	@Bean
	public Map<String, Long> btsIdMapping() {
		Map<String, Long> mapping = new HashMap<>(2);
		mapIntegrationType(mapping, "jira");
		mapIntegrationType(mapping, "rally");
		return mapping;
	}

	@Bean
	public ItemReader<DBObject> btsMongoReader() {
		if (btsIdMapping().isEmpty()) {
			return () -> null;
		}
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "externalSystem");
		itemReader.setQuery("{ $and : [{'externalSystemType' : {$in : ['JIRA', 'RALLY']}}, { projectRef : ?0 }]}");
		itemReader.setParameterValues(Lists.newArrayList(projectName));
		itemReader.setPageSize(CHUNK_SIZE);
		return itemReader;
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> btsItemProcessor() {
		return item -> {

			Long projectId = cacheableDataService.retrieveProjectId((String) item.get("projectRef"));
			if (projectId == null) {
				return null;
			}

			item.put("id", item.get("_id").toString());
			item.removeField("_id");
			item.removeField("_class");

			item.put("authType", item.get("externalSystemAuth"));
			item.removeField("externalSystemAuth");

			item.put("oauthAccessKey", item.get("accessKey"));
			item.removeField("accessKey");

			item.put("defectFormFields", item.get("fields"));
			item.removeField("fields");

			BasicDBObject params = new BasicDBObject("params", item);
			Long externalSystemType = btsIdMapping().get(((String) item.get("externalSystemType")).toLowerCase());
			if (externalSystemType == null) {
				return null;
			}
			return new BasicDBObject("params", params).append("projectId", projectId)
					.append("integrationId", externalSystemType)
					.append("username", item.get("username"))

					.append("project", ((String) item.get("project")).toLowerCase());

		};
	}

	@Bean
	public Step migrateBtsStep() {
		return stepBuilderFactory.get("bts").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(btsMongoReader())
				.processor(btsItemProcessor())
				.writer(btsItemWriter)
				.listener(chunkCountListener)
				.build();
	}

	private void mapIntegrationType(Map<String, Long> mapping, String integrationName) {
		try {
			Long id = jdbcTemplate.queryForObject("SELECT id FROM integration_type WHERE name = :nm",
					Collections.singletonMap("nm", integrationName),
					Long.class
			);
			mapping.put(integrationName, id);
		} catch (EmptyResultDataAccessException e) {
			LOGGER.debug(String.format("Integration type with name '%s' not found.", integrationName));
		}
	}

}
