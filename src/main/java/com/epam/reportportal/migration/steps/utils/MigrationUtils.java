package com.epam.reportportal.migration.steps.utils;

import com.mongodb.DBObject;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class MigrationUtils {

	public static final String SELECT_USER_ID = "SELECT id FROM users WHERE users.login = :lg";

	public static Timestamp toUtcNullSafe(Date date) {
		if (date == null) {
			return Timestamp.from(Instant.now());
		}
		return Timestamp.valueOf(LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC));
	}

	public static Timestamp toUtc(Date date) {
		if (date == null) {
			return null;
		}
		return Timestamp.valueOf(LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC));
	}

	public static Timestamp toUtc(Long date) {
		if (date == null) {
			return null;
		}
		return Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(date), ZoneOffset.UTC));
	}

	public static MongoItemReader<DBObject> getMongoItemReader(MongoTemplate mongoTemplate, String collection) {
		MongoItemReader<DBObject> mongoItemReader = new MongoItemReader<>();
		mongoItemReader.setTemplate(mongoTemplate);
		mongoItemReader.setTargetType(DBObject.class);
		mongoItemReader.setCollection(collection);
		mongoItemReader.setSort(new HashMap<String, Sort.Direction>() {{
			put("_id", Sort.Direction.ASC);
		}});
		mongoItemReader.setQuery("{}");
		return mongoItemReader;
	}
}
