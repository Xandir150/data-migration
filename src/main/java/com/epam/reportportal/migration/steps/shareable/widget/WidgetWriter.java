package com.epam.reportportal.migration.steps.shareable.widget;

import com.epam.reportportal.migration.steps.shareable.ShareableWriter;
import com.epam.reportportal.migration.steps.CacheableDataService;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.shareable.widget.WidgetStepConfig.CF_MAPPING;
import static com.epam.reportportal.migration.steps.shareable.widget.WidgetStepConfig.TYPE_MAPPING;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@SuppressWarnings("ALL")
@Component
public class WidgetWriter implements ItemWriter<DBObject> {

	private static final String INSERT_WIDGET = "INSERT INTO widget (id, name, description, widget_type, items_count, widget_options) "
			+ "VALUES (:id, :nm, :descr, :wt, :ic, :opt::JSONB)";

	private static final String INSERT_CONTENT_FIELDS = "INSERT INTO content_field (id, field) VALUES (?, ?)";

	private static final String INSERT_WIDGET_FILTER = "INSERT INTO widget_filter (widget_id, filter_id) VALUES (?, ?)";

	@Autowired
	private JdbcTemplate template;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private ShareableWriter shareableWriter;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public void write(List<? extends DBObject> items) {
		Map<String, Long> filterMapping = cacheableDataService.loadFilterIdsMapping(items.stream()
				.map(it -> it.get("applyingFilterId"))
				.filter(Objects::nonNull)
				.map(String.class::cast)
				.filter(it -> ObjectId.isValid(it))
				.map(id -> new ObjectId((String) id))
				.collect(Collectors.toSet()));

		List<Object[]> widgetFilter = new ArrayList<>(items.size());
		List<Object[]> contentFields = new ArrayList<>();

		items.forEach(widget -> {
			Long widgetId = write(widget);
			contentFields.addAll(contentFieldsSqlSources(widgetId,
					(BasicDBList) ((DBObject) widget.get("contentOptions")).get("contentFields")
			));
			if (null != filterMapping.get(widget.get("applyingFilterId"))) {
				widgetFilter.add(new Object[] { widgetId, filterMapping.get(widget.get("applyingFilterId")) });
			}
			storeIdsMapping(widget, widgetId);
		});

		template.batchUpdate(INSERT_WIDGET_FILTER, widgetFilter);
		template.batchUpdate(INSERT_CONTENT_FIELDS, contentFields);
	}

	private Long write(DBObject widget) {
		Long widgetId = shareableWriter.writeShareableEntity(widget);
		insertWidget(widget, widgetId);
		shareableWriter.writeAcl(widget, widgetId, WidgetStepConfig.ACL_CLASS);
		return widgetId;
	}

	private void insertWidget(DBObject widget, Long widgetId) {
		DBObject contentOptions = (DBObject) widget.get("contentOptions");
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", widgetId);
		parameterSource.addValue("nm", widget.get("name"));
		parameterSource.addValue("descr", widget.get("description"));
		parameterSource.addValue("wt", TYPE_MAPPING.get(contentOptions.get("gadgetType")));
		parameterSource.addValue("ic", contentOptions.get("itemsCount"));
		parameterSource.addValue("opt", widget.get("widgetOptions"));

		jdbcTemplate.update(INSERT_WIDGET, parameterSource);

	}

	private List<Object[]> contentFieldsSqlSources(Long widgetId, BasicDBList contentFields) {
		if (contentFields != null) {
			return contentFields.stream().map(cf -> CF_MAPPING.getOrDefault(cf, (String) cf)).map(cf -> {
				if (cf.startsWith("statistics")) {
					return cf.toLowerCase();
				}
				return cf;
			}).map(cf -> new Object[] { widgetId, cf }).collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	private void storeIdsMapping(DBObject widget, Long entityId) {
		mongoTemplate.upsert(Query.query(Criteria.where("_id").is(new ObjectId(widget.get("_id").toString()))),
				Update.update("postgresId", entityId)
						.set("name", widget.get("name"))
						.set("share", widget.get("shared"))
						.set("owner", ((DBObject) widget.get("acl")).get("ownerUserId"))
						.set("type", TYPE_MAPPING.get(((DBObject) widget.get("contentOptions")).get("gadgetType"))),
				"widgetMapping"
		);
	}

}
