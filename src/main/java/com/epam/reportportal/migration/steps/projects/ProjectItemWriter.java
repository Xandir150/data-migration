package com.epam.reportportal.migration.steps.projects;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.toUtc;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("projectItemWriter")
@SuppressWarnings("unchecked")
public class ProjectItemWriter implements ItemWriter<DBObject> {

	private static final String INSERT_PROJECT = "INSERT INTO project (name, project_type, organization, creation_date, metadata) VALUES (:nm, :pt, :org, :cd, :md::JSONB) RETURNING project.id";

	private static final String INSERT_PROJECT_USER = "INSERT INTO project_user (user_id, project_id, project_role) VALUES (:userId, :projectId, :projectRole::PROJECT_ROLE_ENUM)";

	private static final String INSERT_ISSUE_TYPES = "INSERT INTO issue_type (issue_group_id, locator, issue_name, "
			+ "abbreviation, hex_color) VALUES (:groupId, :locator, :longName, :shortName, :hexColor) ON CONFLICT DO NOTHING RETURNING id";

	private static final String INSERT_PROJECT_ISSUE_TYPES = "INSERT INTO issue_type_project (project_id, issue_type_id) VALUES (:pr, :it)";

	private static final String INSERT_DEFAULT_ANALYZER_CONFIG = "INSERT INTO project_attribute(attribute_id, value, project_id) VALUES (5, 1, :pr), (6, 1, :pr), (7, 95, :pr), (8, 4, :pr), (9, FALSE, :pr), (10, FALSE, :pr)";

	private static final String INSERT_PROJECT_ATTRIBUTES = "INSERT INTO project_attribute(attribute_id, value, project_id) VALUES (:attr, :val, :pr)";

	private static final String INSERT_EMAIL_SENDER_CASE = "INSERT INTO sender_case (send_case, project_id) VALUES (:sc,:pr) RETURNING id";

	private static final String INSERT_RECIPIENTS = "INSERT INTO recipients (sender_case_id, recipient) VALUES (:sc, :val)";

	private static final String INSERT_LAUNCH_NAMES = "INSERT INTO launch_names (sender_case_id, launch_name) VALUES (:sc, :val)";

	private static final String INSERT_ATTRIBUTE_RULES = "INSERT INTO launch_attribute_rules (sender_case_id, value) VALUES (:sc, :val)";

	@Value("${rp.launch.keepAttr}")
	private String keepAttr;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	@Qualifier("issueGroups")
	private Map<String, Long> issueGroups;

	@Autowired
	@Qualifier("defaultIssueTypes")
	private Map<String, Long> defaultIssueTypes;

	@Autowired
	@Qualifier("defaultAttributes")
	private Map<String, Long> defaultAttributes;

	@Override
	public void write(List<? extends DBObject> items) {
		items.forEach(project -> {
			Long projectId = writeProject(project);
			writeProjectUsers(project, projectId);
			writeProjectIssueTypes(project, projectId);
			writeProjectConfiguration(project, projectId);
			writeEmailRules(project, projectId);
		});
	}

	private Long writeProject(DBObject project) {
		Map<String, Object> params = new HashMap<>();
		params.put("nm", project.get("_id"));
		params.put("pt", ((DBObject) project.get("configuration")).get("entryType"));
		params.put("org", project.get("customer"));
		params.put(
				"cd",
				Optional.ofNullable(toUtc((Date) project.get("creationDate")))
						.orElse(Timestamp.from(LocalDateTime.now().toInstant(ZoneOffset.UTC)))
		);
		params.put("md", "{\"metadata\": {\"migrated_from\": \"MongoDb\"}}");
		Long projectId = jdbcTemplate.queryForObject(INSERT_PROJECT, params, Long.class);
		jdbcTemplate.update(INSERT_DEFAULT_ANALYZER_CONFIG, Collections.singletonMap("pr", projectId));
		return projectId;
	}

	private void writeProjectUsers(DBObject project, Long projectId) {
		Map<String, Object>[] users = ((List<Map<String, Object>>) project.get("users")).stream()
				.peek(it -> it.put("projectId", projectId))
				.toArray(Map[]::new);
		jdbcTemplate.batchUpdate(INSERT_PROJECT_USER, users);
	}

	private void writeProjectIssueTypes(DBObject project, Long projectId) {
		DBObject subTypes = (DBObject) ((DBObject) project.get("configuration")).get("subTypes");
		Map[] projectIssueTypesParams = issueGroups.keySet()
				.stream()
				.flatMap(issueGroup -> collectIssueTypes(projectId, subTypes, issueGroup).stream())
				.toArray(Map[]::new);
		jdbcTemplate.batchUpdate(INSERT_PROJECT_ISSUE_TYPES, projectIssueTypesParams);
	}

	private List<Map> collectIssueTypes(Long projectId, DBObject subTypes, String issueGroup) {
		return ((List<Map>) subTypes.get(issueGroup)).stream().map(issueType -> {

			Map<String, Long> map = new HashMap<>();
			Long issueTypeId;

			String locator = ((String) issueType.get("locator")).toLowerCase();
			if (defaultIssueTypes.get(locator) != null) {
				issueTypeId = defaultIssueTypes.get(locator);
			} else {
				issueType.put("groupId", issueGroups.get(issueGroup));
				issueTypeId = jdbcTemplate.queryForObject(INSERT_ISSUE_TYPES, issueType, Long.class);
			}
			if (issueTypeId == null) {
				return null;
			}
			map.put("pr", projectId);
			map.put("it", issueTypeId);
			return map;
		}).filter(Objects::nonNull).collect(Collectors.toList());
	}

	private void writeProjectConfiguration(DBObject project, Long projectId) {
		DBObject configuration = (DBObject) project.get("configuration");
		configuration.putAll((Map) configuration.get("analyzerConfig"));
		configuration.putAll((Map) configuration.get("emailConfig"));

		Map[] params = defaultAttributes.keySet().stream().map(attribute -> {

			Map<String, Object> map = new HashMap<>();
			map.put("attr", defaultAttributes.get(attribute));

			Object value = configuration.get(attribute);
			if (value != null) {
				map.put("val", value);
			} else {
				ProjectAttributeEnum projectAttributeEnum = ProjectAttributeEnum.findByAttributeName(attribute).get();
				if (projectAttributeEnum.equals(ProjectAttributeEnum.KEEP_LAUNCHES)) {
					map.put("val", keepAttr);
				}
				map.put("val", projectAttributeEnum.getDefaultValue());
			}

			map.put("pr", projectId);
			return map;
		}).toArray(Map[]::new);

		jdbcTemplate.batchUpdate(INSERT_PROJECT_ATTRIBUTES, params);
	}

	private void writeEmailRules(DBObject project, Long projectId) {
		List<DBObject> emailCases = (List<DBObject>) ((DBObject) ((DBObject) project.get("configuration")).get("emailConfig")).get(
				"emailCases");
		emailCases.forEach(emailCase -> {
			Long sendCaseId = writeSendCase(projectId, emailCase);
			writeEmailRule(emailCase, sendCaseId, "recipients", INSERT_RECIPIENTS);
			writeEmailRule(emailCase, sendCaseId, "tags", INSERT_ATTRIBUTE_RULES);
			writeEmailRule(emailCase, sendCaseId, "launchNames", INSERT_LAUNCH_NAMES);
		});
	}

	private Long writeSendCase(Long projectId, DBObject emailCase) {
		Map<String, Object> params = new HashMap<>(2);
		params.put("pr", projectId);
		params.put("sc", emailCase.get("sendCase"));
		return jdbcTemplate.queryForObject(INSERT_EMAIL_SENDER_CASE, params, Long.class);
	}

	private void writeEmailRule(DBObject emailCase, Long sendCaseId, String rule, String sql) {
		List<String> values = (List<String>) emailCase.get(rule);
		if (!CollectionUtils.isEmpty(values)) {
			Map[] params = values.stream().map(val -> {
				Map<String, Object> map = new HashMap<>();
				map.put("sc", sendCaseId);
				map.put("val", val);
				return map;
			}).toArray(Map[]::new);
			jdbcTemplate.batchUpdate(sql, params);
		}
	}

}
