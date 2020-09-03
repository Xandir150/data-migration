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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * @author Dzianis_Shybeka
 */
public class LocalDataStore implements DataStore {

	private static final Logger logger = LoggerFactory.getLogger(LocalDataStore.class);

	private final String storageRootPath;

	private final String remoteRootPath;

	public LocalDataStore(String storageRootPath, String remoteRootPath) {
		this.storageRootPath = storageRootPath;
		this.remoteRootPath = remoteRootPath;
	}

	@Override
	public String save(String filePath, InputStream inputStream) {

		try {

			Path targetPath = Paths.get(storageRootPath, filePath);
			Path targetDirectory = targetPath.getParent();

			if (!Files.isDirectory(targetDirectory)) {
				Files.createDirectories(targetDirectory);
			}

			logger.debug("Saving to: {} ", targetPath.toAbsolutePath());

			Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING);

			return Paths.get(remoteRootPath, filePath).toString();
		} catch (IOException e) {

			logger.error("Unable to save log file ", e);

			throw new RuntimeException("Unable to save log file");
		}
	}

	@Override
	public void delete(String filePath) {

		try {

			Files.deleteIfExists(Paths.get(filePath));
		} catch (IOException e) {

			logger.error("Unable to delete file ", e);

			throw new RuntimeException("Unable to delete log file");
		}
	}
}
