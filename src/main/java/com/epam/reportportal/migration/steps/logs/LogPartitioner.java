package com.epam.reportportal.migration.steps.logs;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.epam.reportportal.migration.steps.utils.DatePartitioner.prepareExecutionContext;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
@Scope(value = "prototype")
public class LogPartitioner implements Partitioner {

	private int itemSize;

	public void setItemIds(List<String> itemIds) {
		itemSize = itemIds.size();
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		int targetSize = itemSize / gridSize + 1;
		Map<String, ExecutionContext> result = new HashMap<>();
		int number = 0;
		int start = 0;
		int end = start + targetSize;
		while (start <= itemSize) {
			ExecutionContext value = new ExecutionContext();
			result.put("partition" + number, value);

			if (end >= itemSize) {
				end = itemSize;
			}
			value.putInt("minValue", start);
			value.putInt("maxValue", end);
			start += targetSize;
			end += targetSize;
			number++;
		}
		return result;
	}
}
