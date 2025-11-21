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
package fr.cirad.tools.mongo;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

/**
 * The Class AutoIncrementCounter.
 */
@Document(collection="counters")
@TypeAlias("AIC")
public class AutoIncrementCounter
{   
    
    /** The id. */
    @Id private String id;
    
    /** The seq. */
    private int seq;

    
    /**
     * Instantiates a new auto increment counter.
     *
     * @param id the id
     * @param seq the seq
     */
    public AutoIncrementCounter(String id, int seq) {
        super();
        this.id = id;
        this.seq = seq;
    }

    /**
     * Gets the seq.
     *
     * @return the seq
     */
    public int getSeq() {
        return seq;
    }

    /**
     * Sets the seq.
     *
     * @param seq the new seq
     */
    public void setSeq(int seq) {
        this.seq = seq;
    }

    /**
     * Gets the next sequence.
     *
     * @param mongo the mongo
     * @param collectionName the collection name
     * @return the next sequence
     */
    static synchronized public int getNextSequence(MongoOperations mongo, String collectionName)
    {
        AutoIncrementCounter counter = mongo.findAndModify(new Query(Criteria.where("_id").is(collectionName)), new Update().inc("seq", 1), FindAndModifyOptions.options().returnNew(true), AutoIncrementCounter.class);
        if (counter != null)
            return counter.getSeq();
        
        // counters collection contains no data for this type
        counter = new AutoIncrementCounter(collectionName, 1);
        mongo.save(counter);
        return 1;
    }
    
    /**
     * Gets the next sequence.
     *
     * @param mongo the mongo
     * @param documentClass the dcoument class
     * @return the next sequence
     */
    static synchronized public int getNextSequence(MongoOperations mongo, Class documentClass)
    {
        return getNextSequence(mongo, MongoTemplateManager.getMongoCollectionName(documentClass));
    }
    
    /**
     * Sets the counter for the given collection to the specified value.
     * If no counter exists for this collection, it will be created.
     *
     * @param mongo the mongo operations
     * @param collectionName the collection name
     * @param value the value to set the counter to
     * @return the previous counter value, or -1 if no counter existed
     */
    static synchronized public int setCounter(MongoOperations mongo, String collectionName, int value)
    {
        AutoIncrementCounter counter = mongo.findAndModify(
            new Query(Criteria.where("_id").is(collectionName)),
            new Update().set("seq", value),
            FindAndModifyOptions.options().returnNew(true),
            AutoIncrementCounter.class
        );
        
        if (counter != null) {
            return counter.getSeq();
        }
        
        // Counter doesn't exist, create new one
        counter = new AutoIncrementCounter(collectionName, value);
        mongo.save(counter);
        return value;
    }

    /**
     * Sets the counter for the given document class to the specified value.
     * If no counter exists for this collection, it will be created.
     *
     * @param mongo the mongo operations
     * @param documentClass the document class
     * @param value the value to set the counter to
     * @return the previous counter value, or -1 if no counter existed
     */
    static synchronized public int setCounter(MongoOperations mongo, Class documentClass, int value)
    {
        return setCounter(mongo, MongoTemplateManager.getMongoCollectionName(documentClass), value);
    }
}