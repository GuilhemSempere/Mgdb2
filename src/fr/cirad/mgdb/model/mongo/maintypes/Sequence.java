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
 * The Class Sequence.
 */
@Document(collection = "sequences")
@TypeAlias("SQ")
public class Sequence {

    /**
     * full sequence (deprecated, isn't filled)
     */
    public final static String FIELDNAME_SEQUENCE = "sq";
    /**
     * sequence length
     */
    public final static String FIELDNAME_LENGTH = "ln";
    /**
     * assembly name
     */
    public final static String FIELDNAME_ASSEMBLY = "as";
    /**
     * sequence md5 checksum
     */
    public final static String FIELDNAME_CHECKSUM = "cs";
    /**
     * location of the fasta file
     */
    public final static String FIELDNAME_LOCATION = "lc";

    /**
     * The id.
     */
    @Id
    private String id;

    /**
     * The sequence.
     */
    @BsonProperty(FIELDNAME_SEQUENCE)  
    @Field(FIELDNAME_SEQUENCE)
    private String sequence;

    /**
     * length of the sequence
     */
    @BsonProperty(FIELDNAME_LENGTH)
    @Field(FIELDNAME_LENGTH)
    private long length;
    
    /**
     * assembly name
     */
    @BsonProperty(FIELDNAME_ASSEMBLY)
    @Field(FIELDNAME_ASSEMBLY)
    private String assembly;

    /**
     * checksum of the upper-case sequence without spaces
     */
    @BsonProperty(FIELDNAME_CHECKSUM)
    @Field(FIELDNAME_CHECKSUM)
    private String checksum;

    /**
     * location of the fasta file
     */
    @BsonProperty(FIELDNAME_LOCATION)
    @Field(FIELDNAME_LOCATION)
    private String filePath;

    /**
     * Instantiates a new sequence.
     *
     * @param id the id
     * @param length
     */
    public Sequence() {
    }

    /**
     * Instantiates a new sequence.
     *
     * @param id the id
     */
    public Sequence(String id) {
        super();
        this.id = id;
    }
    
    /**
     * Instantiates a new sequence.
     *
     * @param id the id
     * @param sequence the sequence
     * @param length
     * @param checksum
     * @param filePath
     */
    public Sequence(String id, String sequence, long length, String checksum, String filePath) {
        super();
        this.id = id;
        this.sequence = sequence;
        this.length = length;
        this.checksum = checksum;
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
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
     * Gets the sequence.
     *
     * @return the sequence
     */
    public String getSequence() {
        return sequence;
    }

    /**
     * Sets the sequence.
     *
     * @param sequence the new sequence
     */
    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

    /**
     * get the length of the sequence
     *
     * @return long length
     */
    public long getLength() {
        return length;
    }

    /**
     * set the length
     *
     * @param length
     */
    public void setLength(long length) {
        this.length = length;
    }

    public String getAssembly() {
        return assembly;
    }

    public void setAssembly(String assembly) {
        this.assembly = assembly;
    }
    
    /**
     * get the checksum of the sequence
     *
     * @return the checksum
     */
    public String getChecksum() {
        return checksum;
    }

    /**
     * set the checksum
     *
     * @param checksum
     */
    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

}
