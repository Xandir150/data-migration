package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Strings;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.items.ItemsStepConfig.OPTIMIZED_TEST_COLLECTION;
import static java.util.stream.Collectors.toList;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@StepScope
@Component("testItemProcessor")
public class TestItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String TRAIT = "auto:";

	private static final String SELECT_ISSUE_TYPE_ID = "SELECT id FROM issue_type WHERE issue_type.locator = :loc";

	private static final String SELECT_BTS_ID = "SELECT id, params -> 'params' ->> 'project' AS project,  params -> 'params' ->> 'url' "
			+ "AS url FROM integration WHERE params -> 'params' ->> 'id' = :mid";

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private MongoOperations mongoOperations;

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
		if (((String) item.get("uniqueId")).startsWith(TRAIT)) {
			generateNewUniqueId(item);
		}
		return item;
	}

	private void generateNewUniqueId(DBObject item) {
		String forEncoding = prepareForEncoding(item);
		String uniqueId = TRAIT + DigestUtils.md5Hex(forEncoding);
		item.put("uniqueId", uniqueId);
	}

	private String prepareForEncoding(DBObject item) {
		Long projectId = (Long) item.get("projectId");
		String launchName = (String) item.get("launchName");
		List<String> pathNames = findPathNames((List<String>) item.get("path"));
		String itemName = (String) item.get("name");
		StringJoiner joiner = new StringJoiner(";");
		joiner.add(projectId.toString()).add(launchName);
		if (!CollectionUtils.isEmpty(pathNames)) {
			joiner.add(String.join(";", pathNames));
		}
		joiner.add(itemName);
		BasicDBList parameters = (BasicDBList) item.get("parameters");
		if (!CollectionUtils.isEmpty(parameters)) {
			joiner.add(parameters.stream()
					.map(p -> (DBObject) p)
					.map(parameter -> (!Strings.isNullOrEmpty((String) parameter.get("key")) ? parameter.get("key") + "=" : "")
							+ parameter.get("value"))
					.collect(Collectors.joining(",")));
		}
		return joiner.toString();
	}

	private DBObject retrieveLaunch(DBObject item) {
		DBObject launchId = cacheableDataService.retrieveLaunchId((String) item.get("launchRef"));
		if (launchId == null) {
			LOGGER.debug("Test item with missed launch is ignored");
			return null;
		}
		item.put("launchId", launchId.get("launchId"));
		item.put("launchName", launchId.get("launchName"));
		item.put("projectId", launchId.get("projectId"));
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

	public List<String> findPathNames(Iterable<String> path) {
		Query q = query(where("_id").in(toObjId(path)));
		q.fields().include("name");
		return mongoOperations.find(q, DBObject.class, OPTIMIZED_TEST_COLLECTION)
				.stream()
				.map(it -> (String) it.get("name"))
				.collect(toList());
	}

	;

	private Collection<ObjectId> toObjId(Iterable<String> path) {
		List<ObjectId> ids = new ArrayList<>();
		for (String id : path) {
			ids.add(new ObjectId(id));
		}
		return ids;
	}
}
