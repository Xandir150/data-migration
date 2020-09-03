package com.epam.reportportal.migration.steps.shareable;

import com.epam.reportportal.migration.steps.CacheableDataService;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class ShareableUtilService {

	@Autowired
	private CacheableDataService cacheableDataService;

	public boolean processShareableEntity(DBObject item) {
		if (!retrieveProject(item)) {
			return false;
		}
		if (!retrieveAcl(item)) {
			return false;
		}
		return true;
	}

	private boolean retrieveAcl(DBObject item) {
		DBObject acl = (DBObject) item.get("acl");
		if (acl == null) {
			return false;
		}
		String owner = (String) acl.get("ownerUserId");
		if (owner == null) {
			return false;
		}
		Long ownerId = cacheableDataService.retrieveAclUser(owner);
		if (ownerId == null) {
			return false;
		}
		item.put("ownerId", ownerId);

		BasicDBList entries = (BasicDBList) acl.get("entries");
		if (!CollectionUtils.isEmpty(entries)) {
			item.put("shared", true);
		} else {
			item.put("shared", false);
		}
		return true;
	}

	private boolean retrieveProject(DBObject item) {
		Long projectId = cacheableDataService.retrieveProjectId((String) item.get("projectName"));
		if (projectId == null) {
			return false;
		}
		item.put("projectId", projectId);
		return true;
	}

}
