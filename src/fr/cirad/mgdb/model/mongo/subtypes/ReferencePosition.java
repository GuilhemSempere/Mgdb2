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
package fr.cirad.mgdb.model.mongo.subtypes;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class ReferencePosition.
 */
public class ReferencePosition
{
	
	/** The Constant FIELDNAME_SEQUENCE. */
	public final static String FIELDNAME_SEQUENCE = "ch";
	
	/** The Constant FIELDNAME_START_SITE. */
	public final static String FIELDNAME_START_SITE = "ss";
	
	/** The Constant FIELDNAME_END_SITE. */
	public final static String FIELDNAME_END_SITE = "es";

	/** The sequence. */
	@BsonProperty(FIELDNAME_SEQUENCE)
	@Field(FIELDNAME_SEQUENCE)
	private String sequence;
	
	/** The start site. */
	@BsonProperty(FIELDNAME_START_SITE)
	@Field(FIELDNAME_START_SITE)
	private long startSite;
	
	/** The end site. */
	@BsonProperty(FIELDNAME_END_SITE)
	@Field(FIELDNAME_END_SITE)
	private Long endSite = null;
	
	/**
	 * Instantiates a new reference position.
	 */
	public ReferencePosition()
	{		
	}

	/**
	 * Instantiates a new reference position.
	 *
	 * @param sequence the sequence
	 * @param startSite the start site
	 */
	public ReferencePosition(String sequence, long startSite) {
		super();
		this.sequence = sequence;
		this.startSite = startSite;
	}
	
	/**
	 * Instantiates a new reference position.
	 *
	 * @param sequence the sequence
	 * @param startSite the start site
	 * @param endSite the end site
	 */
	public ReferencePosition(String sequence, long startSite, Long endSite) {
		this(sequence, startSite);
		if (endSite != null && endSite != startSite)
			this.endSite = endSite;
	}
	
	/**
	 * Gets the sequence.
	 *
	 * @return the sequence
	 */
	public String getSequence() {
		return sequence;
	}

	/**
	 * Sets the sequence.
	 *
	 * @param sequence the new sequence
	 */
	public void setSequence(String sequence) {
		this.sequence = sequence;
	}

	/**
	 * Gets the start site.
	 *
	 * @return the start site
	 */
	public long getStartSite() {
		return startSite;
	}

	/**
	 * Sets the start site.
	 *
	 * @param startSite the new start site
	 */
	public void setStartSite(long startSite) {
		this.startSite = startSite;
	}

	/**
	 * Gets the end site.
	 *
	 * @return the end site
	 */
	public Long getEndSite() {
		return endSite;
	}

	/**
	 * Sets the end site.
	 *
	 * @param endSite the new end site
	 */
	public void setEndSite(Long endSite) {
		this.endSite = endSite;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) 
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof ReferencePosition))
			return false;
		
		boolean f1 = ((ReferencePosition)o).getSequence() == getSequence() || (getSequence() != null && getSequence().equals(((ReferencePosition)o).getSequence()));
		boolean f2 = ((ReferencePosition)o).getStartSite() == getStartSite();
		boolean f3 = ((ReferencePosition)o).getEndSite() == getEndSite() || (getEndSite() != null && getEndSite().equals(((ReferencePosition)o).getEndSite()));
		return f1 && f2 && f3;
	}
}