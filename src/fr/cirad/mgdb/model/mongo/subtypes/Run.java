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
 *
 * @author boizet, sempere
 */
public class Run {

    /**
     * The Constant FIELDNAME_PROJECT_ID.
     */
    public final static String FIELDNAME_PROJECT_ID = "pi";

    /**
     * The Constant FIELDNAME_RUNNAME.
     */
    public final static String FIELDNAME_RUNNAME = "rn";

    /**
     * The project id.
     */
    @BsonProperty(FIELDNAME_PROJECT_ID)
    @Field(FIELDNAME_PROJECT_ID)
    private int projectId;

    /**
     * The run name.
     */
    @BsonProperty(FIELDNAME_RUNNAME)
    @Field(FIELDNAME_RUNNAME)
    private String runName;

    public Run() {
    }
    
    public Run(int projectId, String runName) {
        this.projectId = projectId;
        this.runName = runName;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getRunName() {
        return runName;
    }

    public void setRunName(String runName) {
        this.runName = runName;
    }
    
    @Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof Run))
			return false;
		
		return getProjectId() == ((Run) o).getProjectId() && getRunName() != null && getRunName().equals(((Run) o).getRunName());
	}
}
