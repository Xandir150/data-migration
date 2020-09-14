package com.epam.reportportal.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("chunkCountListener")
public class ChunkCountListener implements ChunkListener {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private int loggingInterval = 5000;

	@Override
	public void beforeChunk(ChunkContext context) {
		// Nothing to do here
	}

	@Override
	public void afterChunk(ChunkContext context) {
		int count = context.getStepContext().getStepExecution().getReadCount();
		String stepName = context.getStepContext().getStepName();
		// If the number of records processed so far is a multiple of the logging interval then output a log message.
		if (count > 0 && count % loggingInterval == 0) {
			LOGGER.info(String.format("%d items in '%s' step are processed", count, stepName));
		}
	}

	@Override
	public void afterChunkError(ChunkContext context) {
		// Nothing to do here
	}
}
