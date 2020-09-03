package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.steps.CacheableDataService;
import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@StepScope
@Component("testItemProcessor")
public class TestItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String SELECT_ISSUE_TYPE_ID = "SELECT id FROM issue_type WHERE issue_type.locator = :loc";

	private static final String SELECT_BTS_ID = "SELECT id, params -> 'params' ->> 'project' AS project,  params -> 'params' ->> 'url' "
			+ "AS url FROM integration WHERE params -> 'params' ->> 'id' = :mid";

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private Cache<String, Long> locatorsFieldsCache;

	@Autowired
	private RowMapper<Map> btsRowMapper;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public DBObject process(DBObject item) {
		if (retrieveLaunch(item) == null) {
			return null;
		}
		if (retrieveParent(item) == null) {
			return null;
		}
		retrieveParentPath(item);
		retrieveIssue(item);
		return item;
	}

	private DBObject retrieveLaunch(DBObject item) {
		Long launchId = cacheableDataService.retrieveLaunchId((String) item.get("launchRef"));
		if (launchId == null) {
			LOGGER.debug("Test item with missed launch is ignored");
			return null;
		}
		item.put("launchId", launchId);
		return item;
	}

	private DBObject retrieveParent(DBObject item) {
		String parent = (String) item.get("parent");
		if (parent == null) {
			return item;
		}
		Long parentId = cacheableDataService.retrieveItemId(parent);

		if (parentId == null && (Integer) item.get("pathLevel") != 0) {
			return null;
		}

		item.put("parentId", parentId);
		return item;
	}

	private void retrieveIssue(DBObject item) {
		DBObject issue = (DBObject) item.get("issue");
		if (issue != null) {
			try {
				String locator = ((String) issue.get("issueType")).toLowerCase();
				Long issueTypeId = locatorsFieldsCache.getIfPresent(locator);
				if (issueTypeId == null) {
					issueTypeId = jdbcTemplate.queryForObject(SELECT_ISSUE_TYPE_ID, Collections.singletonMap("loc", locator), Long.class);
					locatorsFieldsCache.put(locator, issueTypeId);
				}
				issue.put("issueTypeId", issueTypeId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Issue type with locator '%s' not found. Used default.", issue));
				issue.put("issueTypeId", 1L);
			}
			BasicDBList tickets = (BasicDBList) issue.get("externalSystemIssues");
			if (!CollectionUtils.isEmpty(tickets)) {
				tickets = retrieveBts(tickets);
				issue.put("externalSystemIssues", tickets);
			}

		}
	}

	private BasicDBList retrieveBts(BasicDBList tickets) {
		return tickets.stream().map(item -> {
			String systemId = (String) ((DBObject) item).get("externalSystemId");
			try {
				Map bts = jdbcTemplate.queryForObject(SELECT_BTS_ID, Collections.singletonMap("mid", systemId), btsRowMapper);
				((DBObject) item).putAll(bts);
				return item;
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Bts with id '%s' not found. It is ignored.", systemId));
				return null;
			}
		}).filter(Objects::nonNull).collect(Collectors.toCollection(BasicDBList::new));
	}

	private void retrieveParentPath(DBObject item) {
		BasicDBList path = (BasicDBList) item.get("path");
		String pathStr;
		try {
			pathStr = path.stream().map(mongoId -> {
				Long id = cacheableDataService.retrieveItemId((String) mongoId);
				return String.valueOf(id);
			}).collect(Collectors.joining("."));
			item.put("pathIds", pathStr);
		} catch (EmptyResultDataAccessException e) {
			LOGGER.debug(String.format("Item in path '%s' not found. Item is ignored.", path.toString()));
		}
	}
}
