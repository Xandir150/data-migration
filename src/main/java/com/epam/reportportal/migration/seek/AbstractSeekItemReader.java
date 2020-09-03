package com.epam.reportportal.migration.seek;

import org.springframework.util.ClassUtils;

import java.util.Iterator;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public abstract class AbstractSeekItemReader<T> extends AbstractDateStreamItemReader {

	protected int limit = 10;

	protected Iterator<T> results;

	public AbstractSeekItemReader() {
		setName(ClassUtils.getShortName(AbstractSeekItemReader.class));
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	@Override
	protected T doRead() throws Exception {
		if (results == null || !results.hasNext()) {
			results = doPageRead();
			if (results == null || !results.hasNext()) {
				return null;
			}
		}
		return results.next();
	}

	protected abstract Iterator<T> doPageRead();

	@Override
	protected void doOpen() throws Exception {

	}

	@Override
	protected void doClose() throws Exception {

	}

}
