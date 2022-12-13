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

import java.util.LinkedHashMap;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class Individual.
 */
@Document(collection = "individuals")
@TypeAlias("I")
public class Individual implements Comparable<Individual> {

    /**
     * The Constant FIELDNAME_POPULATION.
     */
    public final static String FIELDNAME_POPULATION = "po";

    /**
     * The Constant FIELDNAME_PROBLEM.
     */
    public final static String FIELDNAME_PROBLEM = "pb";

    /**
     * The Constant SECTION_ADDITIONAL_INFO.
     */
    public final static String SECTION_ADDITIONAL_INFO = "ai";

    /**
     * The id.
     */
    @Id
    private String id;

    /**
     * The population.
     */
    @Field(FIELDNAME_POPULATION)
    private String population;

    /**
     * The problems.
     */
    @Field(FIELDNAME_PROBLEM)
    private LinkedHashMap<Integer /*project id*/, String /*problem description*/> problems;

    /**
     * The additional info.
     */
    @Field(SECTION_ADDITIONAL_INFO)
    private LinkedHashMap<String, Object> additionalInfo = null;

    /**
     * Instantiates a new individual.
     *
     * @param id the id
     */
    public Individual(String id) {
        this.id = id;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId() {
        return id;
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
     * Gets the population.
     *
     * @return the population
     */
    public String getPopulation() {
        return population;
    }

    /**
     * Sets the population.
     *
     * @param population the new population
     */
    public void setPopulation(String population) {
        this.population = population;
    }

    /**
     * Gets the problems.
     *
     * @return the problems
     */
    private LinkedHashMap<Integer, String> getProblems() {
        if (problems == null) {
            problems = new LinkedHashMap<Integer, String>();
        }
        return problems;
    }

    /**
     * Gets the problematic projects.
     *
     * @return the problematic projects
     */
    public Set<Integer> getProblematicProjects() {
        return getProblems().keySet();
    }

    /**
     * Checks if is problematic.
     *
     * @return true, if is problematic
     */
    public boolean isProblematic() {
        return getProblems().keySet().size() > 0;
    }

    /**
     * Checks if is problematic in project.
     *
     * @param projId the proj id
     * @return true, if is problematic in project
     */
    public boolean isProblematicInProject(int projId) {
        return getProblems().get(projId) != null;
    }

    /**
     * Sets the problem.
     *
     * @param projId the proj id
     * @param problem the problem
     */
    public void setProblem(int projId, String problem) {
        if (problem == null) {
            getProblems().remove(projId);
        } else {
            getProblems().put(projId, problem);
        }
    }

    /**
     * Gets the problem.
     *
     * @param projId the proj id
     * @return the problem
     */
    public String getProblem(int projId) {
        return getProblems().get(projId);
    }

    /**
     * Gets the additional info.
     *
     * @return the additional info
     */
    public LinkedHashMap<String, Object> getAdditionalInfo() {
        if (additionalInfo == null) {
            additionalInfo = new LinkedHashMap<String, Object>();
        }
        return additionalInfo;
    }

    /**
     * Sets the additional info.
     *
     * @param additionalInfo the additional info
     */
    public void setAdditionalInfo(LinkedHashMap<String, Object> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    /* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof Individual)) {
            return false;
        }

        return getId().equals(((Individual) o).getId());
    }

    @Override
    public int compareTo(Individual other) {
        return getId().compareToIgnoreCase(other.getId());
    }

    @Override
    public String toString() {
        return id;
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
