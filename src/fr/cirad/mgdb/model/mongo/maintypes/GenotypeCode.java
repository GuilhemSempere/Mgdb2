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

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * Stores the mapping between a numeric genotype code and its allele combination.
 * _id is the numeric code, gt is the sorted allele index string e.g. "0/1/2".
 * Codes -99 to 99 are reserved for biallelic genotypes.
 * Codes <= -100 are multiallelic het, sequential downward from -100.
 * Codes >= 100 are multiallelic hom, sequential upward from 100.
 */
@Document(collection = "genotypeCodes")
@TypeAlias("GC")
public class GenotypeCode {

    @Id
    private int id;

    @BsonProperty("gt")
    @Field("gt")
    private String gt;

    public GenotypeCode() {}

    public GenotypeCode(int id, String gt) {
        this.id = id;
        this.gt = gt;
    }

    public int getId() { return id; }
    public String getGt() { return gt; }
    public void setGt(String gt) { this.gt = gt; }
}