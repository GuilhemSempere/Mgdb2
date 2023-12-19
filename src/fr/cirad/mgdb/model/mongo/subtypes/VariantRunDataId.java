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

import fr.cirad.tools.Helper;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 *
 * @author boizet
 */
public class VariantRunDataId  extends Run {

    /**
     * The Constant FIELDNAME_VARIANT_ID.
     */
    public final static String FIELDNAME_VARIANT_ID = "vi";

    /**
     * The variant id.
     */
    @BsonProperty(FIELDNAME_VARIANT_ID)
    @Field(FIELDNAME_VARIANT_ID)
    private String variantId;

    public VariantRunDataId() {
    }

    /**
     * Instantiates a new variant run data id.
     *
     * @param projectId the project id
     * @param runName the run name
     * @param variantId the variant id
     */
    public VariantRunDataId(int projectId, String runName, String variantId) {
        this.setProjectId(projectId);
        this.setRunName(runName);
        this.variantId = variantId;
    }

    /**
     * Gets the variant id.
     *
     * @return the variant id
     */
    public String getVariantId() {
        return variantId;
    }

    public void setVariantId(String variantId) {
        this.variantId = variantId;
    }   
    

    @Override
    public boolean equals(Object o) // thanks to this overriding, HashSet.contains will find such objects based on their ID
    {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof VariantRunDataId)) {
            return false;
        }

        return getProjectId() == ((VariantRunDataId) o).getProjectId() && getRunName().equals(((VariantRunDataId) o).getRunName()) && getVariantId().equals(((VariantRunDataId) o).getVariantId());
    }

    @Override
    public int hashCode() // thanks to this overriding, HashSet.contains will find such objects based on their ID
    {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return this.getProjectId() + Helper.ID_SEPARATOR + this.getRunName() + Helper.ID_SEPARATOR + variantId;
    }
}
