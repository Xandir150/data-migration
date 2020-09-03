package com.epam.reportportal.migration.steps.items;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.CollectionUtils;

import java.util.Date;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.toUtc;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class TestProviderUtils {

	static final ItemSqlParameterSourceProvider<DBObject> RETRY_SOURCE_PROVIDER = item -> {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("uid", item.get("_id").toString());
		parameterSource.addValue("nm", item.get("name"));
		parameterSource.addValue("tp", item.get("type"));
		parameterSource.addValue("st", toUtc((Date) item.get("start_time")));
		parameterSource.addValue("descr", item.get("itemDescription"));
		parameterSource.addValue("lm", toUtc((Date) item.get("last_modified")));
		parameterSource.addValue("uq", item.get("uniqueId"));
		parameterSource.addValue("thash", item.get("uniqueId").hashCode());
		parameterSource.addValue("ch", item.get("has_childs"));
		parameterSource.addValue("par", item.get("parentId"));
		return parameterSource;
	};

	static final ItemSqlParameterSourceProvider<DBObject> TEST_SOURCE_PROVIDER = item -> {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("uid", item.get("_id").toString());
		parameterSource.addValue("nm", item.get("name"));
		parameterSource.addValue("tp", item.get("type"));
		parameterSource.addValue("st", toUtc((Date) item.get("start_time")));
		parameterSource.addValue("descr", item.get("itemDescription"));
		parameterSource.addValue("lm", toUtc((Date) item.get("last_modified")));
		parameterSource.addValue("uq", item.get("uniqueId"));
		parameterSource.addValue("thash", item.get("uniqueId").hashCode());
		parameterSource.addValue("ch", item.get("has_childs"));
		BasicDBList retries = (BasicDBList) item.get("retries");
		if (!CollectionUtils.isEmpty(retries)) {
			parameterSource.addValue("rtr", true);
		} else {
			parameterSource.addValue("rtr", false);
		}
		parameterSource.addValue("par", item.get("parentId"));
		parameterSource.addValue("lid", item.get("launchId"));
		return parameterSource;
	};

	public static final ItemSqlParameterSourceProvider<DBObject> TICKETS_SOURCE_PROVIDER = ticket -> {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("tid", ticket.get("ticketId"));
		parameterSource.addValue("sub", ticket.get("submitter"));
		parameterSource.addValue("sd", toUtc((Long) ticket.get("submitDate")));
		parameterSource.addValue("burl", ticket.get("burl"));
		parameterSource.addValue("bpr", ticket.get("project"));
		parameterSource.addValue("url", ticket.get("url"));
		return parameterSource;
	};

}
