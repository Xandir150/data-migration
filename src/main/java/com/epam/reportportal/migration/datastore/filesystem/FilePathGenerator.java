/*
 * Copyright (C) 2018 EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.reportportal.migration.datastore.filesystem;

import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.time.LocalDateTime;

/**
 * @author Dzianis_Shybeka
 */
@Component
public class FilePathGenerator {

	/**
	 * Generate relative file path for new local file. ${Day of the year}/${split UUID part}
	 *
	 * @return
	 */
	public String generate(LocalDateTime localDateTime, String projectId, String launchUuid) {
		String date = localDateTime.getYear() + "-" + localDateTime.getMonthValue();
		return Paths.get(String.valueOf(projectId), date, launchUuid).toString();
	}
}
