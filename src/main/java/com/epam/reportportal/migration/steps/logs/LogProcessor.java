package com.epam.reportportal.migration.steps.logs;

import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
@StepScope
public class LogProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private GridFsOperations gridFs;

	@Value("${rp.attach.keepFrom}")
	private String keepFrom;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public DBObject process(DBObject log) {
		if (retrieveIds(log) == null) {
			return null;
		}
		if (retrieveBinaryContent(log) == null) {
			return null;
		}
		return log;
	}

	private DBObject retrieveBinaryContent(DBObject log) {
		BasicDBObject binaryContent = (BasicDBObject) log.get("binary_content");
		if (binaryContent != null) {
			GridFSDBFile file = gridFs.findOne(Query.query(Criteria.where("_id")
					.is(new ObjectId((String) binaryContent.get("id")))
					.and("uploadDate")
					.gte(Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant()))));
			if (file == null) {
				return log;
			}
			Long projectId = cacheableDataService.retrieveProjectId((String) file.getMetaData().get("project"));
			if (projectId == null) {
				return null;
			}
			log.put("projectId", projectId);
			log.put("file", file);
		}
		return log;
	}

	private DBObject retrieveIds(DBObject log) {
		DBObject ids = cacheableDataService.retrieveIds((String) log.get("testItemRef"));
		if (ids == null) {
			return null;
		}
		log.putAll(ids);
		return log;
	}
}
