package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.CacheableDataService;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.SELECT_USER_ID;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("launchItemProcessor")
@StepScope
public class LaunchItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public DBObject process(DBObject item) {
		try {
			Long userID = jdbcTemplate.queryForObject(SELECT_USER_ID, Collections.singletonMap("lg", item.get("userRef")), Long.class);
			item.put("userId", userID);
		} catch (EmptyResultDataAccessException e) {
			LOGGER.debug(String.format("User with name '%s' not found", item.get("userRef")));
		}
		Long projectId = cacheableDataService.retrieveProjectId((String) item.get("projectRef"));
		if (projectId == null) {
			LOGGER.debug(String.format("Project %s is missed. Skipping launch with id %s.",
					item.get("projectRef"),
					item.get("_id").toString()
			));
			return null;
		}
		item.put("projectId", projectId);
		return item;
	}
}
