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

import java.util.HashMap;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.Helper;

/**
 * The Class VariantRunData.
 */
@Document(collection = "variantRunData")
@TypeAlias("R")

public class VariantRunData extends AbstractVariantData
{
	/** The Constant FIELDNAME_SAMPLEGENOTYPES. */
	public final static String FIELDNAME_SAMPLEGENOTYPES = "sp";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME = "EFF_nm";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE = "EFF_ge";

	/** The id. */
	@BsonProperty("_id")
	@Id
	private VariantRunDataId id;

	/** The sample genotypes. */
	@BsonProperty(FIELDNAME_SAMPLEGENOTYPES)
	@Field(FIELDNAME_SAMPLEGENOTYPES)
	private HashMap<Integer, SampleGenotype> sampleGenotypes = new HashMap<Integer, SampleGenotype>();

	/**
	 * Instantiates a new variant run data.
	 */
	public VariantRunData() {
	}

	/**
	 * Instantiates a new variant run data.
	 *
	 * @param id the id
	 */
	public VariantRunData(VariantRunDataId id) {
		setId(id);
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public VariantRunDataId getId() {
		return id;
	}

	/**
	 * Sets the id.
	 *
	 * @param id the new id
	 */
	public void setId(VariantRunDataId id) {
		this.id = id;
	}
	
        @Override
	public String getVariantId() {
		return getId().getVariantId();
	}

	/**
	 * Gets the run name.
	 *
	 * @return the run name
	 */
	public String getRunName() {
		return getId().getRunName();
	}

	/**
	 * Gets the sample genotypes.
	 *
	 * @return the sample genotypes
	 */
	public HashMap<Integer, SampleGenotype> getSampleGenotypes() {
		return sampleGenotypes;
	}

	/**
	 * Sets the sample genotypes.
	 *
	 * @param genotypes the genotypes
	 */
	public void setSampleGenotypes(HashMap<Integer, SampleGenotype> genotypes) {
		this.sampleGenotypes = genotypes;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
    @Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof VariantRunData))
			return false;
		
		return getId().equals(((VariantRunData)o).getId());
	}
    
	@Override
	public int hashCode()	// thanks to this overriding, HashSet.contains will find such objects based on their ID
	{
		if (getId() == null)
			return super.hashCode();

		return getId().hashCode();
	}
	
	@Override
	public String toString()
	{
		if (getId() == null)
			return super.toString();

		return getId().toString();
	}
}
