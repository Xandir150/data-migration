package com.epam.reportportal.migration.steps;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.epam.reportportal.migration.steps.StatisticsFieldsService.*;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class CommonItemWriter {

	@Autowired
	private StatisticsFieldsService fieldsService;

	public List<SqlParameterSource> getAttributes(BasicDBList tags, Long id) {
		if (CollectionUtils.isEmpty(tags)) {
			return Collections.emptyList();
		}
		return tags.stream().map(it -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("val", it);
			params.addValue("id", id);
			return params;
		}).collect(Collectors.toList());
	}

	public List<SqlParameterSource> getStatisticsParams(DBObject statistics, Long id) {
		DBObject executions = (DBObject) statistics.get("executionCounter");

		SqlParameterSource[] executionParams = executions.keySet().stream().map(it -> {
			Integer counter = (Integer) executions.get(it);
			return populateValues(id, fieldsService.getPredefinedStatisticsFieldId(it), counter);
		}).filter(Objects::nonNull).toArray(SqlParameterSource[]::new);

		DBObject defects = (DBObject) statistics.get("issueCounter");

		return Stream.of(
				executionParams,
				prepareParams("automationBug", AB_CUSTOM, id, defects),
				prepareParams("productBug", PB_CUSTOM, id, defects),
				prepareParams("systemIssue", SI_CUSTOM, id, defects),
				prepareParams("toInvestigate", TI_CUSTOM, id, defects),
				prepareParams("noDefect", ND_CUSTOM, id, defects)
		).flatMap(Arrays::stream).collect(Collectors.toList());
	}

	private SqlParameterSource[] prepareParams(String type, String fieldRegex, Long id, DBObject defects) {
		DBObject bugType = (DBObject) defects.get(type);
		return bugType.keySet().stream().map(concreteField -> {
			Long defectFieldId = fieldsService.getStatisticsFieldId(fieldRegex, concreteField);
			Integer counter = (Integer) bugType.get(concreteField);
			return populateValues(id, defectFieldId, counter);
		}).filter(Objects::nonNull).toArray(SqlParameterSource[]::new);
	}

	private MapSqlParameterSource populateValues(Long id, Long fieldId, Integer counter) {
		if (counter > 0) {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("ct", counter);
			params.addValue("id", id);
			params.addValue("sfi", fieldId);
			return params;
		}
		return null;
	}

	public List<SqlParameterSource> getParams(BasicDBList parameters, Long itemId) {
		if (CollectionUtils.isEmpty(parameters)) {
			return Collections.emptyList();
		}
		return parameters.stream().map(it -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("key", Optional.ofNullable(((DBObject) it).get("key")).orElse(""));
			params.addValue("val", Optional.ofNullable(((DBObject) it).get("value")).orElse("NULL"));
			params.addValue("id", itemId);
			return params;
		}).collect(Collectors.toList());
	}
}
