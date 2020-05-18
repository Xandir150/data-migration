package com.epam.reportportal.migration.steps.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.items.TestProviderUtils.TICKETS_SOURCE_PROVIDER;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class CacheableDataService {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String SELECT_PROJECT_ID = "SELECT id FROM project WHERE project.name = :name";

	private static final String SELECT_USER_ID = "SELECT id FROM users WHERE users.login = :name";

	private static final String SELECT_ACL_SID = "SELECT id FROM acl_sid WHERE sid = :name";

	private static final String SELECT_LAUNCH_ID = "SELECT id FROM launch WHERE launch.uuid = :uid";

	private static final String SELECT_ITEM_ID = "SELECT item_id FROM test_item WHERE test_item.uuid = :uid";

	private static final String SELECT_IDS = "SELECT item_id, launch_id FROM test_item WHERE test_item.uuid = :uid";

	private static final String INSERT_TICKET = "INSERT INTO ticket (ticket_id, submitter, submit_date, bts_url, bts_project, url) VALUES "
			+ "(:tid, :sub, :sd, :burl, :bpr, :url) RETURNING ticket.id";

	@Autowired
	private Cache<String, Object> idsCache;

	@Autowired
	private Cache<String, Long> usersCache;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private MongoTemplate mongoTemplate;

	public void putMapping(String objectId, Long postgresId) {
		if (objectId != null && postgresId != null) {
			idsCache.put(objectId, postgresId);
		}
	}

	public Long retrieveProjectId(String projectName) {
		Long projectId = (Long) idsCache.getIfPresent(projectName);
		if (projectId == null) {
			try {
				projectId = jdbcTemplate.queryForObject(SELECT_PROJECT_ID, Collections.singletonMap("name", projectName), Long.class);
				idsCache.put(projectName, projectId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Project with name '%s' not found.", projectName));
				return null;
			}
		}
		return projectId;
	}

	public Long retrieveAclUser(String userName) {
		try {
			return jdbcTemplate.queryForObject(SELECT_ACL_SID, Collections.singletonMap("name", userName), Long.class);
		} catch (EmptyResultDataAccessException e) {
			LOGGER.debug(String.format("User with name '%s' not found.", userName));
			return null;
		}
	}

	public Long retrieveUser(String userName) {
		if (userName == null) {
			return null;
		}
		Long userId = usersCache.getIfPresent(userName);
		if (userId == null) {
			try {
				userId = jdbcTemplate.queryForObject(SELECT_USER_ID, Collections.singletonMap("name", userName), Long.class);
				idsCache.put(userName, userId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("User with name '%s' not found.", userName));
				return null;
			}
		}
		return userId;
	}

	public Long retrieveLaunchId(String launchRef) {
		if (launchRef == null) {
			return null;
		}
		Long launchId = (Long) idsCache.getIfPresent(launchRef);
		if (launchId == null) {
			try {
				launchId = jdbcTemplate.queryForObject(SELECT_LAUNCH_ID, Collections.singletonMap("uid", launchRef), Long.class);
				idsCache.put(launchRef, launchId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Launch with uuid '%s' not found. It is ignored.", launchRef));
				return null;
			}
		}
		return launchId;
	}

	public Long retrieveItemId(String itemRef) {
		if (itemRef == null) {
			return null;
		}
		Long itemId = (Long) idsCache.getIfPresent(itemRef);
		if (itemId == null) {
			try {
				itemId = jdbcTemplate.queryForObject(SELECT_ITEM_ID, Collections.singletonMap("uid", itemRef), Long.class);
				idsCache.put(itemRef, itemId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Item with uuid '%s' not found. It is ignored.", itemRef));
				return null;
			}
		}
		return itemId;
	}

	public DBObject retrieveIds(String itemRef) {
		DBObject ids = null;
		Object object = idsCache.getIfPresent(itemRef);
		if (object instanceof DBObject) {
			ids = (DBObject) object;
		}
		if (ids == null) {
			try {
				ids = jdbcTemplate.query(SELECT_IDS, Collections.singletonMap("uid", itemRef), (ResultSetExtractor<DBObject>) rs -> {
					BasicDBObject dbObject = new BasicDBObject();
					if (rs.next()) {
						dbObject.put("itemId", rs.getLong("item_id"));
						dbObject.put("launchId", rs.getLong("launch_id"));
					} else {
						throw new EmptyResultDataAccessException(1);
					}
					return dbObject;
				});
				idsCache.put(itemRef, ids);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("TestItem with uuid '%s' not found. Log is ignored.", itemRef));
				return null;
			}
		}
		return ids;
	}

	public Long retrieveTicketId(DBObject ticket) {
		String url = (String) ticket.get("url");
		Long ticketId = (Long) idsCache.getIfPresent(url);
		if (ticketId == null) {
			try {
				ticketId = jdbcTemplate.queryForObject("SELECT id FROM ticket WHERE url = :url",
						Collections.singletonMap("url", url),
						Long.class
				);
			} catch (Exception e) {
				ticketId = jdbcTemplate.queryForObject(INSERT_TICKET, TICKETS_SOURCE_PROVIDER.createSqlParameterSource(ticket), Long.class);
			}
			idsCache.put(url, ticketId);
		}
		return ticketId;
	}

	public Map<String, Long> loadFilterIdsMapping(Set<ObjectId> mongoIds) {
		Query query = Query.query(Criteria.where("_id").in(mongoIds));
		return mongoTemplate.find(query, DBObject.class, "filterMapping")
				.stream()
				.collect(Collectors.toMap(it -> it.get("_id").toString(), it -> (Long) it.get("postgresId")));
	}
}
