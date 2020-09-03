package com.epam.reportportal.migration.steps.settings;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class SettingsConfig {

	private static final String INSERT_SERVER_SETTINGS = "INSERT INTO server_settings (key, value) VALUES (?,?) ON CONFLICT DO NOTHING";
	static final String INSERT_INTEGRATION = "INSERT INTO integration (name, type, enabled, params, creator) VALUES (:nm,:tp,:en,:par::JSONB,:cr) ON CONFLICT DO NOTHING";
	private static final Long EMAIL_INTEGRAION_ID = 2L;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Bean
	public MongoItemReader<DBObject> settingsReader() {
		return MigrationUtils.getMongoItemReader(mongoTemplate, "serverSettings");
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> settingsProcessor() {
		return item -> item;
	}

	@Bean
	public ItemWriter<? super DBObject> settingsWriter() {
		return items -> items.forEach(item -> {
			writeEmailIntegration(item);
			writeServerSettings(item);
//				writeGithubSettings(item);
//				writeSaml(item);
		});
	}

	private void writeSaml(DBObject item) {
		DBObject samlProviderDetails = (DBObject) item.get("samlProviderDetails");
		if (samlProviderDetails != null) {
			samlProviderDetails.toMap().keySet().stream().forEach(saml -> {
				DBObject provider = (DBObject) samlProviderDetails.get((String) saml);
				jdbcTemplate.update(
						"INSERT INTO saml_provider_details (idp_name, idp_metadata_url, idp_name_id, idp_alias, idp_url, "
								+ "full_name_attribute_id, first_name_attribute_id, last_name_attribute_id, email_attribute_id, enabled) "
								+ "VALUES (?,?,?,?,?,?,?,?,?,?)",
						provider.get("idpName"),
						provider.get("idpMetadata"),
						provider.get("idpNameId"),
						provider.get("idpAlias"),
						provider.get("idpUrl"),
						provider.get("firstNameAttributeId") + " " + provider.get("lastNameAttributeId"),
						provider.get("firstNameAttributeId"),
						provider.get("lastNameAttributeId"),
						provider.get("emailAttributeId"),
						provider.get("enabled")
				);
			});
		}
	}

	private void writeGithubSettings(DBObject item) {
		DBObject oAuth2LoginDetails = (DBObject) item.get("oAuth2LoginDetails");
		if (oAuth2LoginDetails != null) {
			DBObject github = (DBObject) oAuth2LoginDetails.get("github");
			if (github != null) {
				jdbcTemplate.update(
						"INSERT INTO oauth_registration (id, client_id, client_secret, client_auth_method, auth_grant_type, "
								+ "redirect_uri_template, authorization_uri, token_uri, user_info_endpoint_uri, user_info_endpoint_name_attr, "
								+ "jwk_set_uri, client_name) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						"github",
						github.get("clientId"),
						github.get("clientSecret"),
						github.get("clientAuthenticationScheme"),
						github.get("grantType"),
						"{baseUrl}/{action}/oauth2/code/{registrationId}",
						github.get("userAuthorizationUri"),
						github.get("accessTokenUri"),
						"https://api.github.com/user",
						"id",
						null,
						"github"
				);

				List<Object[]> params = ((BasicDBList) github.get("scope")).stream()
						.map(scope -> new Object[] { "github", scope })
						.collect(Collectors.toList());
				jdbcTemplate.batchUpdate("INSERT INTO oauth_registration_scope (oauth_registration_fk, scope) VALUES (?, ?)", params);
			}
		}
	}

	private void writeServerSettings(DBObject item) {
		Boolean analytics = (Boolean) ((DBObject) ((DBObject) item.get("analyticsDetails")).get("all")).get("enabled");
		List<Object[]> objects = new ArrayList<>();
		objects.add(new Object[] { "server.analytics.all", analytics });
		objects.add(new Object[] { "server.details.instance", item.get("instanceId") });
		jdbcTemplate.batchUpdate(INSERT_SERVER_SETTINGS, objects);
	}

	private void writeEmailIntegration(DBObject dbObject) {
		DBObject serverEmailDetails = (DBObject) dbObject.get("serverEmailDetails");
		if (serverEmailDetails != null) {
			BasicDBObject params = new BasicDBObject().append("params", serverEmailDetails);
			MapSqlParameterSource paramsSource = new MapSqlParameterSource();
			paramsSource.addValue("nm", "email server");
			paramsSource.addValue("tp", EMAIL_INTEGRAION_ID);
			paramsSource.addValue("en", serverEmailDetails.get("enabled"));
			paramsSource.addValue("par", params.toString());
			Object creator = serverEmailDetails.get("username");
			if (creator == null) {
				creator = serverEmailDetails.get("from");
			}
			paramsSource.addValue("cr", Optional.ofNullable(creator).orElse(""));
			namedParameterJdbcTemplate.update(INSERT_INTEGRATION, paramsSource);
		}
	}

	@Bean
	public Step migrateSettingsStep() {
		return stepBuilderFactory.get("settings").<DBObject, DBObject>chunk(1).reader(settingsReader())
				.processor(settingsProcessor())
				.writer(settingsWriter())
				.build();
	}

}
