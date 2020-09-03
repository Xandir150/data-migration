package com.epam.reportportal.migration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Pavel Bortnik
 */
@Configuration
@EnableBatchProcessing
public class JobsConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private ApplicationContext ctx;

	@Autowired
	private MigrationJobExecutionListener migrationJobExecutionListener;

	@Bean
	@Scope(value = "prototype")
	public SimpleJobBuilder simpleJobBuilder(String projectNm) {
		SimpleJobBuilder job = jobBuilderFactory.get(projectNm)
				.listener(migrationJobExecutionListener)
				.start((Step) ctx.getBean("migrateUsersStep", projectNm))
				.next((Step) ctx.getBean("migrateProjectsStep", projectNm))
				.next((Step) ctx.getBean("migrateSettingsStep", projectNm))
//				.next(migrateAuthStep)
				.next((Step) ctx.getBean("migrateFilterStep", projectNm))
				.next((Step) ctx.getBean("migrateWidgetStep", projectNm))
				.next((Step) ctx.getBean("migrateDashboardStep", projectNm))
				.next((Step) ctx.getBean("migratePreferencesStep", projectNm))
				.next((Step) ctx.getBean("migrateBtsStep", projectNm))
				.next((Step) ctx.getBean("migrateLaunchStep", projectNm))
				.next((Step) ctx.getBean("migrateLaunchNumberStep", projectNm));
//		for (Step s : levelItemsFlow) {
//			job = job.next(s);
//		}
//		job.next(migrateLogStep);
		return job;
	}

}
