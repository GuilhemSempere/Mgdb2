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

import java.util.LinkedHashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class Sample.
 */
@Document(collection = "indSamples")
@TypeAlias("IS")
public class GenotypingSample {

	public final static String FIELDNAME_INDIVIDUAL = "in";
    public final static String SECTION_ADDITIONAL_INFO = "ai";

	/** The sample id. */
	@Id
	private String id;

	/** The individual. */
	@Field(FIELDNAME_INDIVIDUAL)
	private String individual;

	@Transient
	private boolean detached = false;

	/**
     * The additional info.
     */
    @Field(SECTION_ADDITIONAL_INFO)
    private LinkedHashMap<String, Object> additionalInfo = null;

	/**
	 * Instantiates a new GenotypingSample.
	 *
	 * @param sampleId the sample id
	 * @param individual the individual
	 */
	public GenotypingSample(String sampleId, String individual) {
		this.id = sampleId;
		this.individual = individual;
	}

	public String getIndividual() {
		return detached ? getId().toString() : individual;
	}

//	public void setIndividual(String individual) {
//		this.individual = individual;
//	}

    public boolean isDetached() {
		return detached;
	}

	public void setDetached(boolean detached) {
		this.detached = detached;
	}

    public LinkedHashMap<String, Object> getAdditionalInfo() {
        if (additionalInfo == null) {
            additionalInfo = new LinkedHashMap<String, Object>();
        }
        return additionalInfo;
    }

    public void setAdditionalInfo(LinkedHashMap<String, Object> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public void detachFromIndividual() {
    	detached = true;
    }

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof GenotypingSample))
			return false;
		
		boolean f1 = getIndividual() == ((GenotypingSample) o).getIndividual() || (getIndividual() != null && getIndividual().equals(((GenotypingSample) o).getIndividual()));
        return f1;
	}

    public GenotypingSample() {
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

	public String getId() {
		return id;
	}
}