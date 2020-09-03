/*
 * Copyright 2019 EPAM Systems
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

package com.epam.reportportal.migration.datastore.filesystem.distributed.minio;

import com.epam.reportportal.migration.datastore.filesystem.DataStore;
import com.google.common.collect.Maps;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author <a href="mailto:ivan_budayeu@epam.com">Ivan Budayeu</a>
 */
public class MinioDataStore implements DataStore {

	public static final String BUCKET_PREFIX = "prj-";
	public static final String DEFAULT_BUCKET = "rp-bucket";

	private static final Logger LOGGER = LoggerFactory.getLogger(MinioDataStore.class);
	private static final Lock CREATE_BUCKET_LOCK = new ReentrantLock();

	private final MinioClient minioClient;

	public MinioDataStore(MinioClient minioClient) {
		this.minioClient = minioClient;
	}

	@Override
	public void delete(String filePath) {
		MinioFile minioFile = getMinioFile(filePath);
		try {
			minioClient.removeObject(minioFile.getBucket(), minioFile.getFilePath());
		} catch (Exception e) {
			LOGGER.error("Unable to delete file ", e);
			throw new RuntimeException("Unable to delete file");
		}
	}

	@Override
	public String save(String filePath, InputStream inputStream) {
		MinioFile minioFile = getMinioFile(filePath);
		try {
			if (!minioClient.bucketExists(minioFile.getBucket())) {
				CREATE_BUCKET_LOCK.lock();
				try {
					if (!minioClient.bucketExists(minioFile.getBucket())) {
						minioClient.makeBucket(minioFile.getBucket());
					}
				} finally {
					CREATE_BUCKET_LOCK.unlock();
				}
			}

			minioClient.putObject(minioFile.getBucket(), minioFile.getFilePath(), inputStream, inputStream.available(), Maps.newHashMap());
			return Paths.get(filePath).toString();
		} catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException | XmlPullParserException e) {
			LOGGER.error("Unable to save file ", e);
			throw new RuntimeException("Unable to save file");
		}
	}

	private MinioFile getMinioFile(String filePath) {
		Path targetPath = Paths.get(filePath);
		int nameCount = targetPath.getNameCount();
		if (nameCount > 1) {
			return new MinioFile(BUCKET_PREFIX + retrievePath(targetPath, 0, 1), retrievePath(targetPath, 1, nameCount));
		} else {
			return new MinioFile(DEFAULT_BUCKET, retrievePath(targetPath, 0, 1));
		}

	}

	private String retrievePath(Path path, int beginIndex, int endIndex) {
		return String.valueOf(path.subpath(beginIndex, endIndex));
	}
}
