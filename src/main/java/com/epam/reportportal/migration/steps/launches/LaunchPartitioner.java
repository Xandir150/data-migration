package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.utils.DatePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
@Scope(value = "prototype")
public class LaunchPartitioner implements Partitioner {

	@Autowired
	private MongoOperations mongoOperations;

	@Value("${rp.launch.keepFrom}")
	private String keepFrom;

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());
		Date minDate = new Date();
		Date maxDate = new Date();

		mongoOperations.executeQuery(
				Query.query(Criteria.where("start_time").gte(fromDate)).with(new Sort(Sort.Direction.ASC, "start_time")).limit(1),
				"launch",
				dbObject -> minDate.setTime(((Date) dbObject.get("start_time")).getTime())
		);

		mongoOperations.executeQuery(
				Query.query(Criteria.where("start_time").gte(fromDate)).with(new Sort(Sort.Direction.DESC, "start_time")).limit(1),
				"launch",
				dbObject -> maxDate.setTime(((Date) dbObject.get("start_time")).getTime())
		);

		return DatePartitioner.prepareExecutionContext(gridSize, minDate, maxDate);
	}
}
