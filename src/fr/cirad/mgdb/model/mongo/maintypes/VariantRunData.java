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
import java.util.List;
import java.util.NoSuchElementException;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.SetUniqueListWithConstructor;

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
	
    /**
     * Safely gets known alleles (retrieves eventual missing alleles from corresponding VariantData document)
     *
     * @param mongoTemplate the MongoTemplate to use for fixing allele list if incomplete
     * @throws Exception the exception
     */
	@Override
	public SetUniqueListWithConstructor<String> safelyGetKnownAlleles(MongoTemplate mongoTemplate) throws NoSuchElementException
    {
        if (knownAlleles == null || knownAlleles.isEmpty())
        	fixKnownAlleles(mongoTemplate);	// looks like this run doesn't know any alleles for the given variant
        return getKnownAlleles();
    }
	
    /**
     * Safely gets the alleles from genotype code (retrieves eventual missing alleles from corresponding VariantData document)
     *
     * @param code the code
     * @param mongoTemplate the MongoTemplate to use for fixing allele list if incomplete
     * @return the alleles from genotype code
     * @throws Exception the exception
     */
    public List<String> safelyGetAllelesFromGenotypeCode(String code, MongoTemplate mongoTemplate) throws NoSuchElementException
    {
        try {
            return staticGetAllelesFromGenotypeCode(safelyGetKnownAlleles(mongoTemplate), code);
        }
        catch (NoSuchElementException e1) {	// looks like only some alleles were known by this run
        	fixKnownAlleles(mongoTemplate);
            try {
                return staticGetAllelesFromGenotypeCode(getKnownAlleles(), code);
            }
            catch (NoSuchElementException e2) {
                throw new NoSuchElementException("Variant " + this + " - " + e2.getMessage());
            }
        }
    }

	private void fixKnownAlleles(MongoTemplate mongoTemplate) {
    	knownAlleles = mongoTemplate.findById(getVariantId(), VariantData.class).getKnownAlleles();
        if (knownAlleles != null) {	// fix current document
            Query q = new Query(new Criteria().andOperator(
            		Criteria.where("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID).is(id.getVariantId()),
            		Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(id.getProjectId()),
            		Criteria.where("_id." + VariantRunDataId.FIELDNAME_RUNNAME).is(id.getRunName())
            ));
            mongoTemplate.updateFirst(q, new Update().set(FIELDNAME_KNOWN_ALLELES, knownAlleles), VariantRunData.class);
        }
	}
}