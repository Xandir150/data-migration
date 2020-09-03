package com.epam.reportportal.migration.steps.projects;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public enum ProjectAttributeEnum {

	NOTIFICATIONS_ENABLED("emailEnabled", String.valueOf(false)),
	INTERRUPT_JOB_TIME("interruptJobTime", String.valueOf(Duration.ofDays(1).toSeconds())),
	KEEP_LAUNCHES("keepLaunches", String.valueOf(Duration.ofDays(90).toSeconds())),
	KEEP_LOGS("keepLogs", String.valueOf(Duration.ofDays(90).toSeconds())),
	KEEP_SCREENSHOTS("keepScreenshots", String.valueOf(Duration.ofDays(14).toSeconds())),
	INDEXING_RUNNING("indexingRunning", String.valueOf(false)),
	AUTO_ANALYZER_ENABLED("isAutoAnalyzerEnabled", String.valueOf(false)),
	AUTO_ANALYZER_MODE("analyzerMode", "LAUNCH_NAME");

	private String attribute;
	private String defaultValue;

	ProjectAttributeEnum(String attribute, String defaultValue) {
		this.attribute = attribute;
		this.defaultValue = defaultValue;
	}

	public static Optional<ProjectAttributeEnum> findByAttributeName(String attributeName) {
		return Arrays.stream(ProjectAttributeEnum.values()).filter(v -> v.getAttribute().equalsIgnoreCase(attributeName)).findAny();
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public String getAttribute() {
		return attribute;
	}

}
