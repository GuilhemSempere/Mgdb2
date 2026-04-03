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
import java.util.ArrayList;
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
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataV3Id;
import fr.cirad.tools.SetUniqueListWithConstructor;

/**
 * The Class VariantRunDataV3.
 */
@Document(collection = "VariantRunDataV3")
@TypeAlias("R")

public class VariantRunDataV3 extends AbstractVariantData
{
	/** The Constant FIELDNAME_SAMPLEGENOTYPES. */
	public final static String FIELDNAME_SAMPLEGENOTYPES = "sp";
	public final static String SECTION_ADDITIONAL_INFO = "ai";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME = "EFF_nm";
	
	/** The Constant FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE. */
	public final static String FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE = "EFF_ge";

	/** The id. */
	@BsonProperty("_id")
	@Id
	private VariantRunDataV3Id id;

    /** The sample genotypes. sp[projectIndex][runIndex][callsetIndex] = numeric genotype code (null if missing) */
    @BsonProperty(FIELDNAME_SAMPLEGENOTYPES)
    @Field(FIELDNAME_SAMPLEGENOTYPES)
    private List<List<List<Integer>>> sampleGenotypes = new ArrayList<>();

	/** The additional information on each genotype. ai[projectIndex][runIndex][callsetIndex] = Object (null if missing) */
	@BsonProperty(SECTION_ADDITIONAL_INFO)
	@Field(SECTION_ADDITIONAL_INFO)
	private List<List<List<HashMap<String, Object>>>> additionalInformation = new ArrayList<>();

	/**
	 * Instantiates a new variant run data.
	 */
	public VariantRunDataV3() {
	}

	/**
	 * Instantiates a new variant run data.
	 *
	 * @param id the id
	 */
	public VariantRunDataV3(VariantRunDataV3Id id) {
		setId(id);
	}

	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public VariantRunDataV3Id getId() {
		return id;
	}

	/**
	 * Sets the id.
	 *
	 * @param id the new id
	 */
	public void setId(VariantRunDataV3Id id) {
		this.id = id;
	}
	
        @Override
	public String getVariantId() {
		return getId().getVariantId();
	}


	/**
	 * Gets the sample genotypes.
	 *
	 * @return the sample genotypes
	 */
	public List<List<List<Integer>>> getSampleGenotypes()
    {
        return sampleGenotypes;
    }

	/**
	 * Sets the sample genotypes.
	 *
	 * @param genotypes the genotypes
	 */
	public void setSampleGenotypes(List<List<List<Integer>>> genotypes) {
		this.sampleGenotypes = genotypes;
	}

    public void setGenotype(int projectIndex, int runIndex, int callsetIndex, Integer gt) {
    while (sampleGenotypes.size() <= projectIndex)
        sampleGenotypes.add(new ArrayList<>());
    List<List<Integer>> runsForProject = sampleGenotypes.get(projectIndex);
    while (runsForProject.size() <= runIndex)
        runsForProject.add(new ArrayList<>());
    List<Integer> genotypesForRun = runsForProject.get(runIndex);
    while (genotypesForRun.size() <= callsetIndex)
        genotypesForRun.add(null);
    genotypesForRun.set(callsetIndex, gt);
}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
    @Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof VariantRunDataV3))
			return false;
		
		return getId().equals(((VariantRunDataV3)o).getId());
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
    if (knownAlleles != null) {
        Query q = new Query(Criteria.where("_id." + VariantRunDataV3Id.FIELDNAME_VARIANT_ID).is(id.getVariantId()));
        mongoTemplate.updateFirst(q, new Update().set(FIELDNAME_KNOWN_ALLELES, knownAlleles), VariantRunDataV3.class);
    }
}

    public List<List<List<HashMap<String, Object>>>> getAdditionalInformation() {
        return additionalInformation==null ? new ArrayList<>() : additionalInformation;
    }

    public void setAdditionalInformation(List<List<List<HashMap<String, Object>>>> additionalInformation) {
        this.additionalInformation = additionalInformation;
    }
	public void setAdditionalInformation(int projectIndex, int runIndex, int callsetIndex, HashMap<String, Object> ai) {

		while (additionalInformation.size() <= projectIndex)
			additionalInformation.add(new ArrayList<>());

		List<List<HashMap<String, Object>>> runsForProject = additionalInformation.get(projectIndex);

		while (runsForProject.size() <= runIndex)
			runsForProject.add(new ArrayList<>());

		List<HashMap<String, Object>> aiForRun = runsForProject.get(runIndex);

		while (aiForRun.size() <= callsetIndex)
			aiForRun.add(null);

		aiForRun.set(callsetIndex, ai);
	}
}