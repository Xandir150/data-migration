package com.epam.reportportal.migration.seek;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.util.Assert;

import java.util.Date;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public abstract class AbstractDateStreamItemReader<T> extends AbstractItemStreamItemReader<T> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String OBJECT_DATE = "object.date";
	private static final String OBJECT_LATEST_DATE = "object.latest.date";

	private String dateField;

	private String currentId;

	private Date latestDate;

	private Date currentDate;

	public void setDateField(String dateField) {
		this.dateField = dateField;
	}

	/**
	 * Read next item from input.
	 *
	 * @return item
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected abstract T doRead() throws Exception;

	/**
	 * Open resources necessary to start reading input.
	 *
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected abstract void doOpen() throws Exception;

	/**
	 * Close the resources opened in {@link #doOpen()}.
	 *
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected abstract void doClose() throws Exception;

	/**
	 * Move to the given item index. Subclasses should override this method if
	 * there is a more efficient way of moving to given index than re-reading
	 * the input using {@link #doRead()}.
	 *
	 * @param itemIndex index of item (0 based) to jump to.
	 * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
	 */
	protected void jumpToItem(Date date) throws Exception {
		LOGGER.info("Continue from date " + date);
		currentDate = date;
	}

	public void setLatestDate(Date latestDate) {
		this.latestDate = latestDate;
	}

	public void setCurrentDate(Date currentDate) {
		this.currentDate = currentDate;
	}

	public Date getCurrentDate() {
		return currentDate;
	}

	public String getCurrentId() {
		return currentId;
	}

	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException {
		if (latestDate.getTime() <= currentDate.getTime()) {
			return null;
		}
		T t = doRead();
		if (t instanceof DBObject) {
			currentDate = (Date) ((DBObject) t).get(dateField);
			currentId = ((DBObject) t).get("_id").toString();
		}
		return t;
	}

	@Override
	public void close() throws ItemStreamException {
		super.close();
		currentDate = null;
		try {
			doClose();
		} catch (Exception e) {
			throw new ItemStreamException("Error while closing item reader", e);
		}
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		super.open(executionContext);
		try {
			doOpen();
		} catch (Exception e) {
			throw new ItemStreamException("Failed to initialize the reader", e);
		}

		if (executionContext.containsKey(getExecutionContextKey(OBJECT_LATEST_DATE))) {
			latestDate = (Date) executionContext.get(getExecutionContextKey(OBJECT_LATEST_DATE));
		}

		Date date = null;
		if (executionContext.containsKey(getExecutionContextKey(OBJECT_DATE))) {
			date = (Date) executionContext.get(getExecutionContextKey(OBJECT_DATE));
		} else if (currentDate != null) {
			date = currentDate;
		}

		if (date != null && date.getTime() != latestDate.getTime()) {
			try {
				jumpToItem(date);
			} catch (Exception e) {
				throw new ItemStreamException("Could not move to stored position on restart", e);
			}
		}

		currentDate = date;

	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		super.update(executionContext);
		Assert.notNull(executionContext, "ExecutionContext must not be null");
		executionContext.put(getExecutionContextKey(OBJECT_DATE), currentDate);
		executionContext.put(getExecutionContextKey(OBJECT_LATEST_DATE), latestDate);
	}

}
