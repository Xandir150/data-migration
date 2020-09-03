package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.CommonItemWriter;
import com.epam.reportportal.migration.steps.CacheableDataService;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("launchItemWriter")
@StepScope
public class LaunchItemWriter implements ItemWriter<DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String INSERT_LAUNCH =
			"INSERT INTO launch (uuid, project_id, user_id, name, description, start_time, end_time, number, last_modified,"
					+ "mode, status, approximate_duration, has_retries) VALUES (:uuid, :pr, :usr, :nm, :desc, :start, :end, :num, :last, "
					+ ":md::LAUNCH_MODE_ENUM, :st::STATUS_ENUM, :approx, :hrtr) ON CONFLICT DO NOTHING RETURNING id;";

	private static final String INSERT_LAUNCH_ATTRIBUTES = "INSERT INTO item_attribute (value, launch_id) VALUES (:val, :id)";

	private static final String INSERT_LAUNCH_STATISTICS = "INSERT INTO statistics (s_counter, launch_id, statistics_field_id) VALUES (:ct, :id, :sfi)";

	@Autowired
	private JdbcTemplate jdbc;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private CommonItemWriter commonItemWriter;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public void write(List<? extends DBObject> items) {
		List<SqlParameterSource> attributes = new ArrayList<>();
		List<SqlParameterSource> statistics = new ArrayList<>(items.size() * 4);
		items.forEach(it -> {
			jdbc.execute("SET session_replication_role = REPLICA;");
			try {
				Long id = jdbcTemplate.queryForObject(INSERT_LAUNCH,
						LaunchProviderUtils.LAUNCH_SOURCE_PROVIDER.createSqlParameterSource(it),
						Long.class
				);
				cacheableDataService.putMapping(it.get("_id").toString(), id);
				attributes.addAll(commonItemWriter.getAttributes((BasicDBList) it.get("tags"), id));
				statistics.addAll(commonItemWriter.getStatisticsParams((DBObject) it.get("statistics"), id));
			} catch (Exception e) {
				LOGGER.debug(String.format("Exception while inserting launch with uuid %s", ((ObjectId) it.get("_id")).toString()));
			}
		});
		jdbcTemplate.batchUpdate(INSERT_LAUNCH_ATTRIBUTES, attributes.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_LAUNCH_STATISTICS, statistics.toArray(new SqlParameterSource[0]));
	}
}
