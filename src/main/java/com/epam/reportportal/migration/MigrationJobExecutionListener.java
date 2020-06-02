package com.epam.reportportal.migration;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class MigrationJobExecutionListener implements JobExecutionListener {

	@Autowired
	@Qualifier("threadPoolTaskExecutor")
	private ThreadPoolTaskExecutor taskExecutor;

	@Autowired
	private DataSource dataSource;

	@Override
	public void beforeJob(JobExecution jobExecution) {
		ClassPathResource resource = new ClassPathResource("drop_indexes.sql");
		ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
		databasePopulator.execute(dataSource);
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		taskExecutor.shutdown();
		if (jobExecution.getStatus().equals(BatchStatus.COMPLETED)) {
			ClassPathResource resource = new ClassPathResource("index_create.sql");
			ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
			databasePopulator.execute(dataSource);
		}
		ClassPathResource resource = new ClassPathResource("update_seq.sql");
		ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
		databasePopulator.execute(dataSource);
	}
}
