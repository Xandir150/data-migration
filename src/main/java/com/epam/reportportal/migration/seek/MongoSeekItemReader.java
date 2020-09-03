package com.epam.reportportal.migration.seek;

import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class MongoSeekItemReader<T> extends AbstractSeekItemReader<T> implements InitializingBean {
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final Pattern PLACEHOLDER = Pattern.compile("\\?(\\d+)");
	private MongoOperations template;
	private String query;
	private Class<? extends T> type;
	private Sort sort;
	private String hint;
	private String fields;
	private String collection;
	private List<Object> parameterValues;

	public MongoSeekItemReader() {
		super();
		setName(ClassUtils.getShortName(MongoItemReader.class));
	}

	/**
	 * Used to perform operations against the MongoDB instance.  Also
	 * handles the mapping of documents to objects.
	 *
	 * @param template the MongoOperations instance to use
	 * @see MongoOperations
	 */
	public void setTemplate(MongoOperations template) {
		this.template = template;
	}

	/**
	 * A JSON formatted MongoDB query.  Parameterization of the provided query is allowed
	 * via ?&lt;index&gt; placeholders where the &lt;index&gt; indicates the index of the
	 * parameterValue to substitute.
	 *
	 * @param query JSON formatted Mongo query
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * The type of object to be returned for each {@link #read()} call.
	 *
	 * @param type the type of object to return
	 */
	public void setTargetType(Class<? extends T> type) {
		this.type = type;
	}

	/**
	 * {@link List} of values to be substituted in for each of the
	 * parameters in the query.
	 *
	 * @param parameterValues
	 */
	public void setParameterValues(List<Object> parameterValues) {
		this.parameterValues = parameterValues;
	}

	/**
	 * JSON defining the fields to be returned from the matching documents
	 * by MongoDB.
	 *
	 * @param fields JSON string that identifies the fields to sort by.
	 */
	public void setFields(String fields) {
		this.fields = fields;
	}

	/**
	 * {@link Map} of property names/{@link org.springframework.data.domain.Sort.Direction} values to
	 * sort the input by.
	 *
	 * @param sorts map of properties and direction to sort each.
	 */
	public void setSort(Map<String, Sort.Direction> sorts) {
		this.sort = convertToSort(sorts);
	}

	/**
	 * @param collection Mongo collection to be queried.
	 */
	public void setCollection(String collection) {
		this.collection = collection;
	}

	/**
	 * JSON String telling MongoDB what index to use.
	 *
	 * @param hint string indicating what index to use.
	 */
	public void setHint(String hint) {
		this.hint = hint;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Iterator<T> doPageRead() {

		parameterValues.set(1, getCurrentDate());
		String populatedQuery = replacePlaceholders(query, parameterValues);

		Query mongoQuery = new BasicQuery(populatedQuery);
		if (getCurrentId() != null) {
			mongoQuery.addCriteria(Criteria.where("_id").gt(new ObjectId(getCurrentId())));
		}
		mongoQuery.limit(limit).with(sort);

		if (StringUtils.hasText(collection)) {
			return (Iterator<T>) template.find(mongoQuery, type, collection).iterator();
		} else {
			return (Iterator<T>) template.find(mongoQuery, type).iterator();
		}
	}

	/**
	 * Checks mandatory properties
	 *
	 * @see InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.state(template != null, "An implementation of MongoOperations is required.");
		Assert.state(type != null, "A type to convert the input into is required.");
		Assert.state(query != null, "A query is required.");
		Assert.state(sort != null, "A sort is required.");
	}

	// Copied from StringBasedMongoQuery...is there a place where this type of logic is already exposed?
	private String replacePlaceholders(String input, List<Object> values) {
		Matcher matcher = PLACEHOLDER.matcher(input);
		String result = input;

		while (matcher.find()) {
			String group = matcher.group();
			int index = Integer.parseInt(matcher.group(1));
			result = result.replace(group, getParameterWithIndex(values, index));
		}

		return result;
	}

	// Copied from StringBasedMongoQuery...is there a place where this type of logic is already exposed?
	private String getParameterWithIndex(List<Object> values, int index) {
		return JSON.serialize(values.get(index));
	}

	private Sort convertToSort(Map<String, Sort.Direction> sorts) {
		List<Sort.Order> sortValues = new ArrayList<Sort.Order>();

		for (Map.Entry<String, Sort.Direction> curSort : sorts.entrySet()) {
			sortValues.add(new Sort.Order(curSort.getValue(), curSort.getKey()));
		}

		return new Sort(sortValues);
	}
}
