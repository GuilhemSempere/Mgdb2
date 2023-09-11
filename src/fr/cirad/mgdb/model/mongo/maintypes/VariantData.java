/** *****************************************************************************
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
 ******************************************************************************
 */
package fr.cirad.mgdb.model.mongo.maintypes;

import org.apache.log4j.Logger;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.annotation.Version;
//import org.springframework.data.mongodb.core.index.CompoundIndex;
//import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import java.util.HashSet;
import java.util.Set;
//import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;

/**
 * The Class VariantData.
 */
@Document(collection = "variants")
@TypeAlias("VD")
public class VariantData extends AbstractVariantData {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(VariantData.class);

    /**
     * The id.
     */
    @Id
    protected String id;

    /**
     * The Constant FIELDNAME_VERSION.
     */
    public final static String FIELDNAME_VERSION = "v";

    /**
     * The version.
     */
    @Version
    @Field(FIELDNAME_VERSION)
    private Long version;
    
    
    public final static String FIELDNAME_RUNS = "r";
    
    /**
     * List of runs where the variant has been used.
     */
    @Field(FIELDNAME_RUNS)
    private Set<Run> runs = new HashSet<>();

    /**
     * Instantiates a new variant data.
     */
    public VariantData() {
        super();
    }

    /**
     * Instantiates a new variant data.
     *
     * @param id the id
     */
    public VariantData(String id) {
        setId(id);
    }

    /**
     * Sets the id.
     *
     * @param id the new id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId() {
        return (String) id;
    }

    @Override
    public String getVariantId() {
        return getId();
    }

    /**
     * Gets the version.
     *
     * @return the version
     */
    public Long getVersion() {
        return version;
    }

    /**
     * Sets the version.
     *
     * @param version the new version
     */
    public void setVersion(Long version) {
        this.version = version;
    }

    public Set<Run> getRuns() {
        return runs;
    }

    public void setRuns(Set<Run> runs) {
        this.runs = runs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof VariantData)) {
            return false;
        }

        return getId().equals(((VariantData) o).getId());
    }

    @Override
    public int hashCode() // thanks to this overriding, HashSet.contains will find such objects based on their ID
    {
        if (getId() == null) {
            return super.hashCode();
        }

        return getId().hashCode();
    }
}
