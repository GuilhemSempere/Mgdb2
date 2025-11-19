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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class Assembly.
 */
@Document(collection = "assemblies")
@TypeAlias("A")
public class Assembly {
    
	public final static String FIELDNAME_NAME = "n";

    private static ThreadLocal<Integer> threadAssembly = new ThreadLocal<Integer>();

	public static Integer getThreadBoundAssembly() {
		return threadAssembly.get();
	}

	public static String getVariantRefPosPath(Integer nAssemblyId) {
		return nAssemblyId != null ? AbstractVariantData.FIELDNAME_POSITIONS + "." + nAssemblyId : AbstractVariantData.FIELDNAME_REFERENCE_POSITION;
	}

	public static String getThreadBoundVariantRefPosPath() {
		return getVariantRefPosPath(threadAssembly.get());
	}

	public static String getProjectContigsPath(Integer nAssemblyId) {
		return nAssemblyId != null ? GenotypingProject.FIELDNAME_CONTIGS + "." + nAssemblyId : GenotypingProject.FIELDNAME_SEQUENCES;
	}

	public static String getThreadBoundProjectContigsPath() {
		return getProjectContigsPath(threadAssembly.get());
	}

	public static void setThreadAssembly(Integer threadAssembly) {
		Assembly.threadAssembly.set(threadAssembly);
	}
	
    /**
     * "Safe" wrapper for Assembly.getThreadBoundAssembly()
     * i.e., when null is bound to the thread, if assemblies are defined in the DB, return the first foud one
     * This is a workaround for cases when GA4GH is being invoked by a client that does not know about the "Assembly" request header trick
     * @param sModule
     * @return
     */
	public static Integer safelyGetThreadBoundAssembly(String sModule) {
        Integer nAssembly = Assembly.getThreadBoundAssembly();
        if (nAssembly == null)
            for (Assembly assembly : MongoTemplateManager.get(sModule).findAll(Assembly.class)) {
            	nAssembly = assembly.getId();
            	break;
            }
        return nAssembly;
    }
	
    public static void cleanupThreadAssembly() {
        threadAssembly.remove();
    }
	
    /**
     * The id.
     */
    @Id
    private Integer id;

    /**
     * The description.
     */
    @Indexed
    @Field(FIELDNAME_NAME)
    private String name;

    /**
     * Instantiates a new assembly.
     *
     * @param id the id
     */
    public Assembly(Integer id) {
        super();
        this.id = id;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public Integer getId() {
        return id;
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

}