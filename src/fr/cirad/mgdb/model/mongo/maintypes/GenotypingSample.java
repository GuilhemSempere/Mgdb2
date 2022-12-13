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
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class Sample.
 */
@Document(collection = "samples")
@TypeAlias("GS")
public class GenotypingSample {

	public final static String FIELDNAME_INDEX = "ix";
	public final static String FIELDNAME_NAME = "nm";
	public final static String FIELDNAME_INDIVIDUAL = "in";
	public final static String FIELDNAME_PROJECT_ID = "pj";
	public final static String FIELDNAME_RUN = "rn";
    public final static String SECTION_ADDITIONAL_INFO = "ai";
    
	/** The sample id. */
	@Id
	private int id;

	/** The sample name. */
	@Field(FIELDNAME_NAME)
	private String sampleName;
		
	/** The individual. */
	@Field(FIELDNAME_INDIVIDUAL)
	private String individual;
	
	/** The projectId. */
	@Field(FIELDNAME_PROJECT_ID)
	private int projectId;
	
	/** The run. */
	@Field(FIELDNAME_RUN)
	private String run;
        
    /**
     * The additional info.
     */
    @Field(SECTION_ADDITIONAL_INFO)
    private LinkedHashMap<String, Object> additionalInfo = null;

	/**
	 * Instantiates a new GenotypingSample.
	 *
	 * @param sampleId the sample id
	 * @param projectId the project id
	 * @param run the run name
	 * @param individual the individual
	 * @param sampleName the sampleName
	 */
	public GenotypingSample(int sampleId, int projectId, String run, String individual, String sampleName) {
		this.id = sampleId;
		this.projectId = projectId;
		this.run = run;
		this.individual = individual;
		this.sampleName = sampleName != null ? sampleName : getIndividual() + "-" + getProjectId() + "-" + getRun();
	}
	
	/**
	 * Instantiates a new GenotypingSample.
	 *
	 * @param sampleId the sample id
	 * @param projectId the project id
	 * @param run the run name
	 * @param individual the individual
	 */
	public GenotypingSample(int sampleId, int projectId, String run, String individual) {
		this(sampleId, projectId, run, individual, null);
	}

	public Integer getProjectId() {
		return projectId;
	}

//	public void setProjectId(Integer projectId) {
//		this.projectId = projectId;
//	}

	public String getRun() {
		return run;
	}

//	public void setRun(String run) {
//		this.run = run;
//	}

	public String getIndividual() {
		return individual;
	}

//	public void setIndividual(String individual) {
//		this.individual = individual;
//	}

    public LinkedHashMap<String, Object> getAdditionalInfo() {
        if (additionalInfo == null) {
            additionalInfo = new LinkedHashMap<String, Object>();
        }
        return additionalInfo;
    }

    public void setAdditionalInfo(LinkedHashMap<String, Object> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }
	
	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof GenotypingSample))
			return false;
		
		boolean f1 = getIndividual() == getIndividual() || (getIndividual() != null && getIndividual().equals(getIndividual()));
		boolean f2 = getProjectId() == getProjectId() || (getProjectId() != null && getProjectId().equals(getProjectId()));
		boolean f3 = getRun() == getRun() || (getRun() != null && getRun().equals(getRun()));
		return f1 && f2 && f3;
	}

    public GenotypingSample() {
    }

	public String getSampleName() {
		return sampleName;
	}

	public void setSampleName(String sampleName) {
		this.sampleName = sampleName;
	}

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
	
	public String toString()
	{
		return getSampleName() + "(" + individual + "&curren;" + projectId + "&curren;" + run + ")";
	}

	public Integer getId() {
		return id;
	}
}