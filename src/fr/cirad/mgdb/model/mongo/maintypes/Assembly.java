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

/**
 * The Class Assembly.
 */
@Document(collection = "assemblies")
@TypeAlias("A")
public class Assembly {

//    public final static String FIELDNAME_DATABASE = "d";
    
	public final static String FIELDNAME_NAME = "n";

    /**
     * The id.
     */
    @Id
    private int id;

//    /**
//     * The type.
//     */
//    @Field(FIELDNAME_DATABASE)
//    private String database;

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
    public Assembly(int id) {
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

//	public String getDatabase() {
//		return database;
//	}
//
//	public void setDatabase(String database) {
//		this.database = database;
//	}
//
}
