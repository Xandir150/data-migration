package com.epam.reportportal.migration.steps.items;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.Date;
import java.util.Map;

import static com.epam.reportportal.migration.steps.utils.DatePartitioner.prepareExecutionContext;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class DatePartitioner implements Partitioner {

	private Date minDate;

	private Date maxDate;

	private int pathLevel;

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> result = prepareExecutionContext(gridSize, minDate, maxDate);
		result.values().forEach(context -> context.putInt("pathLevel", pathLevel));
		return result;
	}

	public void setMinDate(Date minDate) {
		this.minDate = minDate;
	}

	public void setMaxDate(Date maxDate) {
		this.maxDate = maxDate;
	}

	public void setPathLevel(int pathLevel) {
		this.pathLevel = pathLevel;
	}

}
