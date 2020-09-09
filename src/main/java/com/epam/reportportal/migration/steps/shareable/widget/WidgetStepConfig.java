package com.epam.reportportal.migration.steps.shareable.widget;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class WidgetStepConfig {

	public static Map<String, String> CF_MAPPING = ImmutableMap.<String, String>builder().put("tags", "attributes")
			.put("start_time", "startTime")
			.put("end_time", "endTime")
			.put("last_modified", "lastModified")
			.build();
	public static Map<String, String> TYPE_MAPPING = ImmutableMap.<String, String>builder().put("old_line_chart", "oldLineChart")
			.put("investigated_trend", "investigatedTrend")
			.put("launch_statistics", "launchStatistics")
			.put("statistic_trend", "statisticTrend")
			.put("cases_trend", "casesTrend")
			.put("not_passed", "notPassed")
			.put("overall_statistics", "overallStatistics")
			.put("unique_bug_table", "uniqueBugTable")
			.put("bug_trend", "bugTrend")
			.put("launches_table", "launchesTable")
			.put("activity_stream", "activityStream")
			.put("launches_comparison_chart", "launchesComparisonChart")
			.put("launches_duration_chart", "launchesDurationChart")
			.put("most_failed_test_cases", "topTestCases")
			.put("flaky_test_cases", "flakyTestCases")
			.put("passing_rate_summary", "passingRateSummary")
			.put("passing_rate_per_launch", "passingRatePerLaunch")
			.put("product_status", "productStatus")
			.put("cumulative", "cumulative")
			.put("most_time_consuming", "mostTimeConsuming")
			.build();

	public static Map<String, String> WIDGET_OPTIONS_MAPPING = ImmutableMap.<String, String>builder().put("filterName", "launchNameFilter")
			.put("start_launch", "startLaunch")
			.put("finish_launch", "finishLaunch")
			.put("delete_launch", "deleteLaunch")
			.put("post_issue", "postIssue")
			.put("link_issue", "linkIssue")
			.put("unlink_issue", "unlinkIssue")
			.put("create_user", "createUser")
			.put("create_dashboard", "createDashboard")
			.put("update_dashboard", "updateDashboard")
			.put("delete_dashboard", "deleteDashboard")
			.put("create_widget", "createWidget")
			.put("update_widget", "updateWidget")
			.put("delete_widget", "deleteWidget")
			.put("create_filter", "createFilter")
			.put("update_filter", "updateFilter")
			.put("delete_filter", "deleteFilter")
			.put("create_bts", "createIntegration")
			.put("update_bts", "updateIntegration")
			.put("delete_bts", "deleteIntegration")
			.put("update_project", "updateProject")
			.put("update_analyzer", "updateAnalyzer")
			.put("generate_index", "generateIndex")
			.put("delete_index", "deleteIndex")
			.put("create_defect", "createDefect")
			.put("update_defect", "updateDefect")
			.put("delete_defect", "deleteDefect")
			.put("start_import", "startImport")
			.put("finish_import", "finishImport")
			.build();

	static Long ACL_CLASS;

	private static final int CHUNK_SIZE = 100;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private ChunkListener chunkCountListener;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private ItemWriter<DBObject> widgetWriter;

	@Autowired
	private ShareableUtilService shareableUtilService;

	@PostConstruct
	public void initialQueries() {
		try {
			if (mongoTemplate.collectionExists("widgetMapping")) {
				mongoTemplate.dropCollection("widgetMapping");
			}
			ACL_CLASS = jdbcTemplate.queryForObject(
					"INSERT INTO acl_class (class, class_id_type) VALUES ('com.epam.ta.reportportal.entity.widget.Widget','java.lang.Long') RETURNING id",
					Long.class
			);
		} catch (Exception e) {
			ACL_CLASS = jdbcTemplate.queryForObject("SELECT id FROM acl_class WHERE class = 'com.epam.ta.reportportal.entity.widget.Widget'",
					Long.class
			);
		}
	}

	public MongoItemReader<DBObject> widgetItemReader(String projectName) {
		MongoItemReader<DBObject> reader = MigrationUtils.getMongoItemReader(mongoTemplate, "widget");
		reader.setQuery("{projectName : ?0}");
		reader.setParameterValues(Lists.newArrayList(projectName));
		reader.setPageSize(CHUNK_SIZE);
		return reader;
	}

	@Bean("migrateWidgetStep")
	@Scope(value = "prototype")
	public Step migrateWidgetStep(String projectName) {
		return stepBuilderFactory.get("widget").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(widgetItemReader(projectName))
				.processor(widgetProcessor())
				.writer(widgetWriter)
				.listener(chunkCountListener)
				.build();
	}

	private ItemProcessor<DBObject, DBObject> widgetProcessor() {
		return widget -> {
			boolean processed = shareableUtilService.processShareableEntity(widget);
			if (!processed) {
				return null;
			}
			DBObject contentOptions = (DBObject) widget.get("contentOptions");
			String widgetOptions = processWidgetOptions(contentOptions);
			if (widgetOptions == null) {
				return null;
			}
			widget.put("widgetOptions", widgetOptions);
			return widget;
		};
	}

	private String processWidgetOptions(DBObject contentOptions) {
		DBObject widgetOptions = (DBObject) contentOptions.get("widgetOptions");

		if (!TYPE_MAPPING.containsKey((String) contentOptions.get("gadgetType"))) {
			return null;
		}

		switch ((String) contentOptions.get("gadgetType")) {

			case "activity_stream":
				if (widgetOptions == null) {
					return null;
				}
				processActivityOptions(widgetOptions);
				break;

			case "investigated_trend":
			case "cases_trend":
				widgetOptions = processTimeline(widgetOptions);
				break;

			case "flaky_test_cases":
			case "most_failed_test_cases":
				widgetOptions = processFlaky(widgetOptions);
				break;

			case "launches_duration_chart":
				widgetOptions = processLatest(widgetOptions);
				break;
			case "overall_statistics":
				widgetOptions = processLatest(widgetOptions);
				widgetOptions = processOverall(widgetOptions);
				break;
			case "passing_rate_per_launch":
				widgetOptions = processLaunchFilterName(widgetOptions);
				widgetOptions = processView(widgetOptions);
				break;
			case "passing_rate_summary":
				widgetOptions = processView(widgetOptions);
				break;
			case "statistic_trend":
				widgetOptions = processTimeline(widgetOptions);
				widgetOptions = processStatsTrend(widgetOptions);
				break;
			case "unique_bug_table":
				widgetOptions = new BasicDBObject().append("latest", false);
				break;
			default:
				widgetOptions = new BasicDBObject();

		}
		return new BasicDBObject("options", widgetOptions).toString();
	}

	private DBObject processStatsTrend(DBObject widgetOptions) {
		BasicDBList basicDBList = (BasicDBList) widgetOptions.get("viewMode");
		if (!CollectionUtils.isEmpty(basicDBList)) {
			String viewMode = (String) basicDBList.get(0);
			widgetOptions.put("viewMode", "bar");
			if (viewMode.equalsIgnoreCase("areaChartMode")) {
				widgetOptions.put("viewMode", "area-spline");
			}
		} else {
			widgetOptions.put("viewMode", "bar");
		}
		widgetOptions.put("zoom", false);
		return widgetOptions;
	}

	private DBObject processView(DBObject widgetOptions) {
		if (widgetOptions.get("viewMode") != null) {
			String viewMode = (String) ((BasicDBList) widgetOptions.get("viewMode")).get(0);
			if (viewMode.equalsIgnoreCase("barMode")) {
				widgetOptions.put("viewMode", "bar");
			} else if (viewMode.equalsIgnoreCase("pieChartMode")) {
				widgetOptions.put("viewMode", "pie");
			}
			return widgetOptions;
		}
		widgetOptions.put("viewMode", "bar");
		return widgetOptions;
	}

	private DBObject processLaunchFilterName(DBObject widgetOptions) {
		if (widgetOptions != null) {
			BasicDBList launchNameFilter = (BasicDBList) widgetOptions.get("launchNameFilter");
			if (!CollectionUtils.isEmpty(launchNameFilter)) {
				widgetOptions.put("launchNameFilter", launchNameFilter.get(0));
			}
			return widgetOptions;
		}
		return new BasicDBObject();
	}

	private DBObject processOverall(DBObject widgetOptions) {
		BasicDBList viewMode = (BasicDBList) widgetOptions.get("viewMode");
		if (viewMode != null && viewMode.get(0) != null) {
			widgetOptions.put("viewMode", viewMode.get(0));
		} else {
			widgetOptions.put("viewMode", "panel");
		}
		return widgetOptions;
	}

	private DBObject processLatest(DBObject widgetOptions) {
		if (widgetOptions != null) {
			Object latest = widgetOptions.get("latest");
			widgetOptions.put("latest", latest != null);
			return widgetOptions;
		}
		return new BasicDBObject("latest", false);
	}

	private DBObject processFlaky(DBObject widgetOptions) {
		if (widgetOptions != null) {
			if (widgetOptions.get("include_methods") != null) {
				widgetOptions.put("includeMethods", true);
				widgetOptions.removeField("include_methods");
			} else {
				widgetOptions.put("includeMethods", false);
			}
			if (widgetOptions.get("launchNameFilter") != null) {
				widgetOptions.put("launchNameFilter", ((BasicDBList) widgetOptions.get("launchNameFilter")).get(0));
			}
			return widgetOptions;
		}
		return new BasicDBObject();
	}

	private DBObject processTimeline(DBObject widgetOptions) {
		if (widgetOptions != null) {
			BasicDBList timeline = (BasicDBList) widgetOptions.get("timeline");
			if (!CollectionUtils.isEmpty(timeline)) {
				String res = timeline.get(0).toString().toLowerCase();
				widgetOptions.put("timeline", res);
			}
			return widgetOptions;
		}
		return new BasicDBObject();
	}

	private void processActivityOptions(DBObject widgetOptions) {
		if (widgetOptions.get("userRef") != null) {
			widgetOptions.put("userRef",
					((BasicDBList) widgetOptions.get("userRef")).stream().map(String.class::cast).collect(Collectors.joining(","))
			);
		}
		widgetOptions.put("actionType",
				((BasicDBList) widgetOptions.get("actionType")).stream()
						.map(String.class::cast)
						.map(it -> WIDGET_OPTIONS_MAPPING.get(it))
						.collect(Collectors.toList())
		);
	}

}
