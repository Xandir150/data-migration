package com.epam.reportportal.migration.steps.items;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class BtsRowMapper implements RowMapper<Map> {

	@Override
	public Map mapRow(ResultSet rs, int rowNum) throws SQLException {
		Map<String, Object> res = new HashMap<>();
		res.put("btsId", rs.getLong("id"));
		res.put("project", rs.getString("project"));
		res.put("burl", rs.getString("url"));
		return res;
	}
}
