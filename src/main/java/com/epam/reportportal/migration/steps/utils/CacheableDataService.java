package com.epam.reportportal.migration.steps.utils;

import com.epam.reportportal.migration.IdPair;
import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;

import static com.epam.reportportal.migration.steps.items.TestProviderUtils.TICKETS_SOURCE_PROVIDER;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class CacheableDataService {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String SELECT_PROJECT_ID = "SELECT id FROM project WHERE project.name = :name";

	private static final String INSERT_TICKET = "INSERT INTO ticket (ticket_id, submitter, submit_date, bts_url, bts_project, url) VALUES "
			+ "(:tid, :sub, :sd, :burl, :bpr, :url) RETURNING ticket.id";

	@Autowired
	private org.ehcache.Cache<String, IdPair> idsCache;

	@Autowired
	@Qualifier("mongoCache")
	private Cache<String, Object> mongoCache;

	@Autowired
	private Cache<String, Long> usersCache;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	public void putMapping(String objectId, Long postgresId) {
		if (objectId != null && postgresId != null) {
			idsCache.put(objectId, new IdPair(postgresId, null));
			mongoCache.put(objectId, postgresId);
		}
	}

	public Long retrieveProjectId(String projectName) {
		Long projectId = usersCache.getIfPresent(projectName);
		if (projectId == null) {
			try {
				projectId = jdbcTemplate.queryForObject(SELECT_PROJECT_ID, Collections.singletonMap("name", projectName), Long.class);
				usersCache.put(projectName, projectId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Project with name '%s' not found.", projectName));
				return null;
			}
		}
		return projectId;
	}

	public Long retrieveLaunchId(String launchRef) {
		if (launchRef == null) {
			return null;
		}
		IdPair idPair = idsCache.get(launchRef);
		if (idPair == null) {
			Long launchId = (Long) mongoCache.getIfPresent(launchRef);
			if (launchId == null) {
				LOGGER.debug(String.format("Launch with uuid '%s' not found. It is ignored.", launchRef));
				return null;
			}
			idsCache.put(launchRef, new IdPair(launchId, null));
		}
		return idPair.getItemId();
	}

	public Long retrieveItemId(String itemRef) {
		if (itemRef == null) {
			return null;
		}
		IdPair pair = idsCache.get(itemRef);
		if (pair == null) {
			DBObject res = (DBObject) mongoCache.getIfPresent(itemRef);
			if (res == null) {
				LOGGER.debug(String.format("Item with uuid '%s' not found. It is ignored.", itemRef));
				return null;
			}
			pair = new IdPair((Long) res.get("itemId"), (Long) res.get("launchId"));
			idsCache.put(itemRef, pair);
		}
		return pair.getItemId();
	}

	public void putIds(String itemRef, Long itemId, Long launchId) {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("itemId", itemId);
		dbObject.put("launchId", launchId);
		mongoCache.put(itemRef, dbObject);
	}

	public IdPair retrieveIds(String itemRef) {
		IdPair pair = idsCache.get(itemRef);
		if (pair == null) {
			DBObject res = (DBObject) mongoCache.getIfPresent(itemRef);
			if (res == null) {
				LOGGER.debug(String.format("TestItem with uuid '%s' not found. Log is ignored.", itemRef));
				return null;
			}
			pair = new IdPair((Long) res.get("itemId"), (Long) res.get("launchId"));
			idsCache.put(itemRef, pair);
		}
		return pair;
	}

	public Long retrieveTicketId(DBObject ticket) {
		String url = (String) ticket.get("url");
		Long ticketId = usersCache.getIfPresent(url);
		if (ticketId == null) {
			try {
				ticketId = jdbcTemplate.queryForObject("SELECT id FROM ticket WHERE url = :url",
						Collections.singletonMap("url", url),
						Long.class
				);
			} catch (Exception e) {
				ticketId = jdbcTemplate.queryForObject(INSERT_TICKET, TICKETS_SOURCE_PROVIDER.createSqlParameterSource(ticket), Long.class);
			}
			usersCache.put(url, ticketId);
		}
		return ticketId;
	}
}
