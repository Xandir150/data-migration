package com.epam.reportportal.migration.steps.launches;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.Date;
import java.util.Optional;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.toUtc;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
class LaunchProviderUtils {

	static final ItemSqlParameterSourceProvider<DBObject> LAUNCH_SOURCE_PROVIDER = item -> {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("uuid", ((ObjectId) item.get("_id")).toString());
		parameterSource.addValue("pr", item.get("projectId"));
		parameterSource.addValue("usr", item.get("userId"));
		parameterSource.addValue("nm", item.get("name"));
		parameterSource.addValue("desc", item.get("description"));
		parameterSource.addValue("start", toUtc((Date) item.get("start_time")));
		parameterSource.addValue("end", toUtc((Date) item.get("end_time")));
		parameterSource.addValue("num", item.get("number"));
		parameterSource.addValue("last", toUtc((Date) item.get("last_modified")));
		parameterSource.addValue("md", item.get("mode"));
		parameterSource.addValue("st", item.get("status"));
		parameterSource.addValue("approx", item.get("approximateDuration"));
		parameterSource.addValue("hrtr", Optional.ofNullable(item.get("hasRetries")).orElse(false));
		return parameterSource;
	};

}
