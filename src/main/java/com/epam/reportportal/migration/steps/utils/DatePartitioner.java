package com.epam.reportportal.migration.steps.utils;

import org.springframework.batch.item.ExecutionContext;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class DatePartitioner {

	public static Map<String, ExecutionContext> prepareExecutionContext(int gridSize, Date minDate, Date maxDate) {
		if (minDate == null || maxDate == null) {
			return new HashMap<>();
		}
		long targetSize = (maxDate.getTime() - minDate.getTime()) / gridSize + 1;
		Map<String, ExecutionContext> result = new HashMap<>();
		int number = 0;
		long start = minDate.getTime();
		long end = start + targetSize - 1;

		while (start <= maxDate.getTime()) {
			ExecutionContext value = new ExecutionContext();
			result.put("partition" + number, value);

			if (end >= maxDate.getTime()) {
				end = maxDate.getTime();
			}
			value.putLong("minValue", start);
			value.putLong("maxValue", end);
			start += targetSize;
			end += targetSize;
			number++;
		}

		return result;
	}

}
