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
 * The Class Population.
 */
@Document(collection = "populations")
@TypeAlias("P")
public class Population
{
	
	/** The Constant FIELDNAME_NAME. */
	public final static String FIELDNAME_NAME = "nm";
	
	/** The Constant FIELDNAME_DESCRIPTION. */
	public final static String FIELDNAME_DESCRIPTION = "ds";
	
	/** The Constant FIELDNAME_POPULATION_GROUP. */
	public final static String FIELDNAME_POPULATION_GROUP = "pg";
	
	/** The Constant FIELDNAME_PARENT_POPULATION. */
	public final static String FIELDNAME_PARENT_POPULATION = "pp";
	
	/**
	* The Constant SECTION_ADDITIONAL_INFO.
	*/
	public final static String SECTION_ADDITIONAL_INFO = "ai";
	
	/** The id. */
	@Id
	private String id;

	/** The name. */
	@Field(FIELDNAME_NAME)
	private String name;	
	
	/** The description. */
	@Field(FIELDNAME_DESCRIPTION)
	private String description;	

	/** The pop group. */
	@Field(FIELDNAME_PARENT_POPULATION)
	private String parentPop;	

	/** The pop group. */
	@Field(FIELDNAME_POPULATION_GROUP)
	private String popGroup;	
	
	/**
	* The additional info.
	*/
	@Field(SECTION_ADDITIONAL_INFO)
	private LinkedHashMap<String, Object> additionalInfo = null;

	/**
	 * Instantiates a new population.
	 *
	 * @param id the id
	 */
	public Population(String id) {
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
	
//	public void setId(String id) {
//		this.id = id;
//	}

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
	 * Checks if is outgroup.
	 *
	 * @return true, if is outgroup
	 */
	public boolean isOutgroup() {
		return "OUT".equals(popGroup);
	}
	
	/**
	 * Gets the pop group.
	 *
	 * @return the pop group
	 */
	public String getPopGroup() {
		return popGroup;
	}

	/**
	 * Sets the pop group.
	 *
	 * @param popGroup the new pop group
	 */
	public void setPopGroup(String popGroup) {
		this.popGroup = popGroup;
	}

	/**
	 * Gets the parent pop.
	 *
	 * @return the parent pop
	 */
	public String getParentPop() {
		return parentPop;
	}
	
	/**
	 * Sets the parent pop.
	 *
	 * @param parentPop the new parent pop
	 */
	public void setParentPop(String parentPop) {
		this.parentPop = parentPop;
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
}
