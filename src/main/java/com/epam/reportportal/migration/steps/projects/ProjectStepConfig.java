package com.epam.reportportal.migration.steps.projects;

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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
@SuppressWarnings("unchecked")
public class ProjectStepConfig {

	private static final int CHUNK_SIZE = 100;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	@Qualifier("projectItemProcessor")
	private ItemProcessor projectItemProcessor;

	@Autowired
	@Qualifier("projectItemWriter")
	private ItemWriter projectItemWriter;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	private TaskExecutor threadPoolTaskExecutor;

	public MongoItemReader<DBObject> projectMongoItemReader(String projectName) {
		MongoItemReader<DBObject> project = MigrationUtils.getMongoItemReader(mongoTemplate, "project");
		project.setQuery("{_id : ?0}");
		project.setParameterValues(Lists.newArrayList(projectName));
		project.setPageSize(CHUNK_SIZE);
		return project;
	}

	@Bean
	@Scope(value = "prototype")
	public Step migrateProjectsStep(String projectName) {
		final TaskletStep project = stepBuilderFactory.get("project").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(projectMongoItemReader(projectName))
				.processor(projectItemProcessor)
				.writer(projectItemWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
//		project.setAllowStartIfComplete(true);
		return project;
	}

	@Bean
	// Issue group have fixed ids in PostgreSQL
	public Map<String, Long> issueGroups() {
		Map<String, Long> groups = new HashMap<>(5);
		groups.put("TO_INVESTIGATE", 1L);
		groups.put("AUTOMATION_BUG", 2L);
		groups.put("PRODUCT_BUG", 3L);
		groups.put("NO_DEFECT", 4L);
		groups.put("SYSTEM_ISSUE", 5L);
		return groups;
	}

	@Bean
	// Default issue types have fixed ids in PostgreSQL
	public Map<String, Long> defaultIssueTypes() {
		Map<String, Long> issueTypes = new HashMap<>(5);
		issueTypes.put("ti001", 1L);
		issueTypes.put("ab001", 2L);
		issueTypes.put("pb001", 3L);
		issueTypes.put("nd001", 4L);
		issueTypes.put("si001", 5L);
		return issueTypes;
	}

	@Bean
	// Default attributes have fixed ids in PostgreSQL
	public Map<String, Long> defaultAttributes() {
		Map<String, Long> attributes = new HashMap<>(14);

		attributes.put("interruptJobTime", 1L);
		attributes.put("keepLaunches", 2L);
		attributes.put("keepLogs", 3L);
		attributes.put("keepScreenshots", 4L);

		attributes.put("isAutoAnalyzerEnabled", 11L);
		attributes.put("analyzerMode", 12L);

		attributes.put("emailEnabled", 13L);
		attributes.put("from", 14L);
		return attributes;
	}
}
