package com.epam.reportportal.migration.steps.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static com.epam.reportportal.migration.JobsConfiguration.CACHE_MAPPING;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("mongoCache")
public class MongoCache implements Cache<String, Object> {

	@Autowired
	private MongoTemplate mongoTemplate;

	private static ReentrantLock lock = new ReentrantLock();

	@Nullable
	@Override
	public Object getIfPresent(Object key) {
		if (key != null) {
			DBObject value = mongoTemplate.findById(key, DBObject.class, CACHE_MAPPING);
			if (value != null) {
				return value.get("value");
			}
			return null;
		}
		return null;
	}

	@Nullable
	@Override
	public Object get(@NonNull String key, @NonNull Function<? super String, ?> mappingFunction) {
		return null;
	}

	@Override
	public @NonNull Map<String, Object> getAllPresent(@NonNull Iterable<?> keys) {
		return null;
	}

	@Override
	public void put(@NonNull String key, @NonNull Object value) {
		if (getIfPresent(key) == null) {
			lock.lock();
			try {
				if (getIfPresent(key) == null) {
					BasicDBObject dbObj = new BasicDBObject();
					dbObj.append("_id", new ObjectId(key));
					dbObj.append("value", value);
					mongoTemplate.insert(dbObj, CACHE_MAPPING);
				}
			} finally {
				lock.unlock();
			}
		}
	}

	@Override
	public void putAll(@NonNull Map<? extends String, ?> map) {

	}

	@Override
	public void invalidate(@NonNull Object key) {

	}

	@Override
	public void invalidateAll(@NonNull Iterable<?> keys) {

	}

	@Override
	public void invalidateAll() {

	}

	@Override
	public @NonNegative long estimatedSize() {
		return 0;
	}

	@Override
	public @NonNull CacheStats stats() {
		return null;
	}

	@Override
	public @NonNull ConcurrentMap<String, Object> asMap() {
		return null;
	}

	@Override
	public void cleanUp() {

	}

	@Override
	public @NonNull Policy<String, Object> policy() {
		return null;
	}

	@Override
	public @NonNull Map<String, Object> getAll(@NonNull Iterable<? extends String> keys,
			@NonNull Function<Iterable<? extends String>, Map<String, Object>> mappingFunction) {
		return null;
	}
}
