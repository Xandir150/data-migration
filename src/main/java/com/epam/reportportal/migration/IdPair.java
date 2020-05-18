package com.epam.reportportal.migration;

import java.io.Serializable;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class IdPair implements Serializable {

	private Long itemId;

	private Long launchId;

	public IdPair(Long itemId, Long launchId) {
		this.itemId = itemId;
		this.launchId = launchId;
	}

	public Long getItemId() {
		return itemId;
	}

	public Long getLaunchId() {
		return launchId;
	}
}
