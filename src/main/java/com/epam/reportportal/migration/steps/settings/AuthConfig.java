package com.epam.reportportal.migration.steps.settings;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import static com.epam.reportportal.migration.steps.settings.SettingsConfig.INSERT_INTEGRATION;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class AuthConfig {

	private static Long LDAP_INTEGRATION_ID = 1L;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Bean
	public MongoItemReader<DBObject> authReader() {
		return MigrationUtils.getMongoItemReader(mongoTemplate, "authConfig");
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> authProcessor() {
		return item -> item;
	}

	@Bean
	public ItemWriter<? super DBObject> authWriter() {
		return items -> items.forEach(item -> {
			writeLdap(item);
			writeAd(item);
		});
	}

	private void writeAd(DBObject item) {
		DBObject ad = (DBObject) item.get("activeDirectory");

		if (ad == null) {
			return;
		}

		MapSqlParameterSource paramsSource = new MapSqlParameterSource();
		paramsSource.addValue("nm", "ad");
		paramsSource.addValue("tp", LDAP_INTEGRATION_ID);
		paramsSource.addValue("en", ad.get("enabled"));
		paramsSource.addValue("par", null);
		paramsSource.addValue("cr", "mongodb");
		Long adId = namedParameterJdbcTemplate.queryForObject(INSERT_INTEGRATION, paramsSource, Long.class);

		DBObject synchronizationAttributes = (DBObject) ad.get("synchronizationAttributes");
		Long syncAttrId = null;

		if (synchronizationAttributes != null) {
			syncAttrId = jdbcTemplate.queryForObject(
					"INSERT INTO ldap_synchronization_attributes (email, full_name, photo) VALUES (?, ?, ?) RETURNING id",
					Long.class,
					synchronizationAttributes.get("email"),
					synchronizationAttributes.get("fullName"),
					synchronizationAttributes.get("photo")
			);
		}

		jdbcTemplate.update(
				"INSERT INTO active_directory_config (id, url, base_dn, sync_attributes_id, domain) VALUES (?,?,?,?,?)",
				adId,
				ad.get("url"),
				ad.get("baseDn"),
				syncAttrId,
				ad.get("domain")
		);

	}

	private void writeLdap(DBObject item) {
		DBObject ldap = (DBObject) item.get("ldap");

		if (ldap == null) {
			return;
		}

		MapSqlParameterSource paramsSource = new MapSqlParameterSource();
		paramsSource.addValue("nm", "ldap");
		paramsSource.addValue("tp", LDAP_INTEGRATION_ID);
		paramsSource.addValue("en", ldap.get("enabled"));
		paramsSource.addValue("par", null);
		paramsSource.addValue("cr", "mongodb");
		Long ldapId = namedParameterJdbcTemplate.queryForObject(INSERT_INTEGRATION, paramsSource, Long.class);

		DBObject synchronizationAttributes = (DBObject) ldap.get("synchronizationAttributes");
		Long syncAttrId = null;

		if (synchronizationAttributes != null) {
			syncAttrId = jdbcTemplate.queryForObject(
					"INSERT INTO ldap_synchronization_attributes (email, full_name, photo) VALUES (?, ?, ?) RETURNING id",
					Long.class,
					synchronizationAttributes.get("email"),
					synchronizationAttributes.get("fullName"),
					synchronizationAttributes.get("photo")
			);
		}

		jdbcTemplate.update(
				"INSERT INTO ldap_config (id, url, base_dn, sync_attributes_id, user_dn_pattern, "
						+ "user_search_filter, group_search_base, group_search_filter, password_attributes, manager_dn, manager_password, passwordencodertype) "
						+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,cast(? AS PASSWORD_ENCODER_TYPE))",
				ldapId,
				ldap.get("url"),
				ldap.get("baseDn"),
				syncAttrId,
				ldap.get("userDnPattern"),
				ldap.get("userSearchFilter"),
				ldap.get("groupSearchBase"),
				ldap.get("groupSearchFilter"),
				ldap.get("passwordAttribute"),
				ldap.get("managerDn"),
				ldap.get("managerPassword"),
				ldap.get("passwordEncoderType")
		);
	}

	@Bean
	public Step migrateAuthStep() {
		return stepBuilderFactory.get("authConfig").<DBObject, DBObject>chunk(1).reader(authReader())
				.processor(authProcessor())
				.writer(authWriter())
				.build();
	}

}
