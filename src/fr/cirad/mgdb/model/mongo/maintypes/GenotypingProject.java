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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.tools.AlphaNumericComparator;

/**
 * The Class GenotypingProject.
 */
@Document(collection = "projects")
@TypeAlias("GP")
public class GenotypingProject {
    /**
     * The Constant FIELDNAME_NAME.
     */
    public final static String FIELDNAME_NAME = "nm";

    /**
     * The Constant FIELDNAME_TYPE.
     */
    public final static String FIELDNAME_TYPE = "ty";

    /**
     * The Constant FIELDNAME_DESCRIPTION.
     */
    public final static String FIELDNAME_DESCRIPTION = "ds";

    /**
     * The Constant FIELDNAME_TECHNOLOGY.
     */
    public final static String FIELDNAME_TECHNOLOGY = "te";

    /**
     * The Constant FIELDNAME_CREATION_DATE.
     */
    public final static String FIELDNAME_CREATION_DATE = "cd";

    /**
     * The Constant FIELDNAME_ORIGIN.
     */
    public final static String FIELDNAME_ORIGIN = "or";

    /**
     * The Constant SECTION_ADDITIONAL_INFO.
     */
    public final static String SECTION_ADDITIONAL_INFO = "ai";

    /**
     * The Constant FIELDNAME_PLOIDY_LEVEL.
     */
    public static final String FIELDNAME_PLOIDY_LEVEL = "pl";

    /**
     * The Constant FIELDNAME_SEQUENCES.
     */
    public static final String FIELDNAME_SEQUENCES = "ch";
    
    /**
     * The Constant FIELDNAME_CONTIGS.
     */
    public static final String FIELDNAME_CONTIGS = "c";

    /**
     * The Constant FIELDNAME_RUNS.
     */
    public static final String FIELDNAME_RUNS = "rn";

    /**
     * The Constant FIELDNAME_ALLELE_COUNTS.
     */
    public static final String FIELDNAME_ALLELE_COUNTS = "ac";

    /**
     * The Constant FIELDNAME_VARIANT_TYPES.
     */
    public static final String FIELDNAME_VARIANT_TYPES = "vt";

    /**
     * The Constant FIELDNAME_EFFECT_ANNOTATIONS.
     */
    public static final String FIELDNAME_EFFECT_ANNOTATIONS = "ea";

    /**
     * The id.
     */
    @Id
    private int id;

    /**
     * The name.
     */
    @Field(FIELDNAME_NAME)
    private String name;

    /**
     * The type.
     */
    @Field(FIELDNAME_TYPE)
    private String type;

    /**
     * The description.
     */
    @Field(FIELDNAME_DESCRIPTION)
    private String description;

    /**
     * The technology.
     */
    @Field(FIELDNAME_TECHNOLOGY)
    private String technology;

    /**
     * The creation date.
     */
    @Field(FIELDNAME_CREATION_DATE)
    private Date creationDate = null;

    /**
     * The origin.
     */
    @Field(FIELDNAME_ORIGIN)
    private int origin;

    /**
     * The additional info.
     */
    @Field(SECTION_ADDITIONAL_INFO)
    private HashMap<String, Comparable> additionalInfo = null;

    /**
     * The ploidy level.
     */
    @Field(FIELDNAME_PLOIDY_LEVEL)
    private int ploidyLevel = 0;

    /**
     * The runs.
     */
    @Field(FIELDNAME_RUNS)
    private List<String> runs = new ArrayList<String>();

    /**
     * The sequences.
     */
    @Field(FIELDNAME_SEQUENCES)
    private TreeSet<String> sequences = new TreeSet<String>(new AlphaNumericComparator());
    
    /**
     * The sequences.
     */
    @Field(FIELDNAME_CONTIGS) /*TODO: not backwards compatible*/
    private Map<Integer, TreeSet<String>> contigs = new HashMap<>();

    /**
     * The allele counts.
     */
    @Field(FIELDNAME_ALLELE_COUNTS)
    private TreeSet<Integer> alleleCounts = new TreeSet<Integer>();

    /**
     * The variant types.
     */
    @Field(FIELDNAME_VARIANT_TYPES)
    private LinkedHashSet<String> variantTypes = new LinkedHashSet<String>();

    /**
     * The effect annotations.
     */
    @Field(FIELDNAME_EFFECT_ANNOTATIONS)
    private TreeSet<String> effectAnnotations = new TreeSet<String>();

    /**
     * Instantiates a new genotyping project.
     *
     * @param id the id
     */
    public GenotypingProject(int id) {
        super();
        this.id = id;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Gets the origin.
     *
     * @return the origin
     */
    public int getOrigin() {
        return origin;
    }

    /**
     * Sets the origin.
     *
     * @param origin the new origin
     */
    public void setOrigin(int origin) {
        this.origin = origin;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name.
     *
     * @param name the new name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type.
     *
     * @param type the new type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets the creation date.
     *
     * @return the creation date
     */
    public Date getCreationDate() {
        return creationDate;
    }

    /**
     * Sets the creation date.
     *
     * @param creationDate the new creation date
     */
    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    /**
     * Gets the description.
     *
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description.
     *
     * @param description the new description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Gets the technology.
     *
     * @return the technology
     */
    public String getTechnology() {
        return technology;
    }

    /**
     * Sets the technology.
     *
     * @param technology the new technology
     */
    public void setTechnology(String technology) {
        this.technology = technology;
    }

    /**
     * Gets the additional info.
     *
     * @return the additional info
     */
    public HashMap<String, Comparable> getAdditionalInfo() {
        if (additionalInfo == null) {
            additionalInfo = new HashMap<String, Comparable>();
        }
        return additionalInfo;
    }

    /**
     * Sets the additional info.
     *
     * @param additionalInfo the additional info
     */
    public void setAdditionalInfo(HashMap<String, Comparable> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    /**
     * Gets the ploidy level.
     *
     * @return the ploidy level
     */
    public int getPloidyLevel() {
        return ploidyLevel;
    }

    /**
     * Sets the ploidy level.
     *
     * @param ploidyLevel the new ploidy level
     */
    public void setPloidyLevel(int ploidyLevel) {
        this.ploidyLevel = ploidyLevel;
    }

    /**
     * Gets the runs.
     *
     * @return the runs
     */
    public List<String> getRuns() {
        return runs;
    }
    
    /**
     * Gets the sequences.
     *
     * @return the sequences
     */
//    public TreeSet<String> getSequences() {
//        return sequences;
//    }

    /**
     * Gets the contigs by assembly ID.
     *
     * @return the contigs by assembly ID
     */
    public Map<Integer, TreeSet<String>> getContigs() {
        return contigs;
    }

    /**
     * Gets the contigs for a given assembly ID.
     *
     * @return the contigs for a given assembly ID
     */
    public TreeSet<String> getContigs(Integer nAssemblyId) {
    	if (nAssemblyId == null)
    		return sequences;

        TreeSet<String> seqs = contigs.get(nAssemblyId);
        if (seqs == null) {
            seqs = new TreeSet<String>(new AlphaNumericComparator());
            contigs.put(nAssemblyId, seqs);
        }
        return seqs;
    }

    /**
     * Gets the allele counts.
     *
     * @return the allele counts
     */
    public TreeSet<Integer> getAlleleCounts() {
        return alleleCounts;
    }

    /**
     * Gets the variant types.
     *
     * @return the variant types
     */
    public LinkedHashSet<String> getVariantTypes() {
        return variantTypes;
    }

    /**
     * Sets the variant types.
     *
     * @param variantTypes the new variant types
     */
    public void setVariantTypes(LinkedHashSet<String> variantTypes) {
        this.variantTypes = variantTypes;
    }

    /**
     * Gets the effect annotations.
     *
     * @return the effect annotations
     */
    public TreeSet<String> getEffectAnnotations() {
        return effectAnnotations;
    }

    public void clearEverythingExceptMetaData()
    {
		getRuns().clear();
		getAlleleCounts().clear();
		getEffectAnnotations().clear();
		getVariantTypes().clear();
		sequences.clear();
		if (getContigs() != null)
			getContigs().clear();
		setPloidyLevel(0);
		getAdditionalInfo().clear();
    }
}
