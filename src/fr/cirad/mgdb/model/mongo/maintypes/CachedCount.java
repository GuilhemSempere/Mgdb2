/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2019, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class CachedCount.
 */
@Document(collection = "cachedCounts")
@TypeAlias("CC")
public class CachedCount {

	public final static String FIELDNAME_CHUNK_COUNTS = "val";
	
	/** The entity id. */
	@Id
	private String id;
	
	/** Result counts for all variant chunks */
	@Field(FIELDNAME_CHUNK_COUNTS)
	private List<Long> chunkCounts = new ArrayList<>();

	public List<Long> getChunkCounts() {
		return chunkCounts;
	}

	/**
	 * Instantiates a new CachedCount.
	 *
	 * @param queryKey the query id
	 * @param counts the list of counts to cache (one per chunk)
	 */
	public CachedCount(String queryKey, List<Long> counts) {
		this.id = queryKey;
		this.chunkCounts = counts;
	}

    public CachedCount() {
    }

	public String getId() {
		return id;
	}
		
	@SuppressWarnings("unchecked")
	static public Long getCachedCount(MongoTemplate mongoTemplate, String queryKey, Class cachedCountType) {
		CachedCount cachedCount = (CachedCount) mongoTemplate.findById(queryKey, cachedCountType == null ? CachedCount.class : cachedCountType);
		if (cachedCount == null)
			return null;
        
		long result = 0;
		for (Long partialCount : cachedCount.getChunkCounts())
			result += partialCount;

        return result;
	}
}