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
 * @author boizet
 */
public class VariantRunDataV3Id {

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

    public VariantRunDataV3Id() {
    }

    /**
     * Instantiates a new variant run data id.
     *
     * @param variantId the variant id
     */
    public VariantRunDataV3Id(String variantId) {
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

        if (o == null || !(o instanceof VariantRunDataV3Id)) {
            return false;
        }

        return getVariantId().equals(((VariantRunDataV3Id) o).getVariantId());
    }

    @Override
    public int hashCode() // thanks to this overriding, HashSet.contains will find such objects based on their ID
    {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return variantId;
    }
}
