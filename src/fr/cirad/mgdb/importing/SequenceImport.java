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
package fr.cirad.mgdb.importing;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.model.mongo.maintypes.Sequence;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class SequenceImport.
 */
public class SequenceImport {

    /**
     * compute md5 and length for a sequence
     *
     * @param seqInfo
     * @param is
     * @param headerList
     * @return the following char (-1 if end of file or '>' if there is another sequence)
     * @throws java.io.IOException
     * @throws java.security.NoSuchAlgorithmException
     */
    public static boolean getMD5andLength(Map<String, String> seqInfo, InputStream is, List<String> headerList) throws IOException, NoSuchAlgorithmException {

        int spaceCode = (int) '\n';
        int chevronCode = (int) '>';
        DigestInputStream dis = null;
        int c = -1;
        int count = 0;
        int lineLength = 0;
        int localLength = 0;
        int total = 0;
        boolean goOn = false;
        MessageDigest md;
        String header = "";
        try {

            while ((c = is.read()) != spaceCode) {
                // skip the header and do not include it in checksum 
                header += (char) c;
            }
            headerList.add(header);

            // start to compute md5
            md = MessageDigest.getInstance("MD5");
            dis = new md5Digest(is, md);
            while ((c = dis.read()) != spaceCode) {
                // get the 2nd line length 
                lineLength++;
            }
            // count this line 
            count++;

            while ((c = dis.read()) != chevronCode && c != -1) {
                localLength++;
                if (c == spaceCode) {

                    // line is shorter than the other 
                    if (localLength < lineLength) {
                        localLength--; // do not count the '\n' 
                    } else {
                        count++;  // count the lines 
                        localLength = 0;
                    }
                }
            }
            total = lineLength * count + localLength;

            goOn = c != -1; // true if c ='>'

        }
        catch (IOException | NoSuchAlgorithmException ex)
        {
            ex.printStackTrace();
            throw ex;
        }
        finally
        {
            byte[] hash = dis.getMessageDigest().digest();
            seqInfo.put(Helper.bytesToHex(hash), Integer.toString(total));
        	if (!goOn && dis != null)
        		dis.close();
        }
        return goOn;
    }

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(SequenceImport.class);

    public static void importSeq(MongoTemplate mongoTemplate, String[] args, String seqCollName) throws Exception
    {
        String lcFile = args[1].toLowerCase();
        InputStream is;
        if (lcFile.startsWith("ftp://") || lcFile.startsWith("http://") || lcFile.startsWith("https://"))
        	is = new URL(args[1]).openStream();	// import from an URL
        else
            is = new BufferedInputStream(new FileInputStream(args[1]));	// import from a local file

	    if (lcFile.endsWith(".gz"))
	    	is = new GZIPInputStream(is);
	    
        List<String> headerList = new ArrayList<>();
        Map<String, String> seqInfo = new LinkedHashMap<>();
        while (getMD5andLength(seqInfo, is, headerList))
        	; // at each iteration, a sequence is processed
        is.close();

        LOG.info("Importing fasta file");
        int rowIndex = 0;

        for (Entry<String, String> entry : seqInfo.entrySet()) {
            String sequenceId = headerList.get(rowIndex);
            // sequenceId looks like :  "10 dna:chromosome chromosome:GRCh38:10:1:133797422:1 REF"
            // sequence name is 10 in this example
            String name = sequenceId.trim();
            if (name.startsWith(">"))
            	name = name.substring(1);
            name = name.trim().split("\\s+")[0];

            String checksum = entry.getKey();
            String length = entry.getValue();
            // do not store the sequence since we don't need it ? 
            mongoTemplate.save(new Sequence(name, null, Long.valueOf(length), checksum, args[1]), seqCollName);
            rowIndex++;
        }
        LOG.info(rowIndex + " records added to collection " + seqCollName);
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new Exception("You must pass 3 parameters as arguments: DATASOURCE name, FASTA reference-file, 3rd parameter only supports values '2' (empty all database's sequence data before importing), and '0' (only overwrite existing sequences)!");
        }

        String sModule = args[0];
        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);

        String seqCollName = MongoTemplateManager.getMongoCollectionName(Sequence.class);
        if ("2".equals(args[2])) {	// empty project's sequence data before importing
            if (mongoTemplate.collectionExists(seqCollName)) {
                mongoTemplate.dropCollection(seqCollName);
                LOG.info("Collection " + seqCollName + " dropped.");
            }
        } else if ("0".equals(args[2])) {
            // do nothing
        } else {
            throw new Exception("3rd parameter only supports values '2' (empty all database's sequence data before importing), and '0' (only overwrite existing sequences)");
        }
        importSeq(mongoTemplate, args, seqCollName);

        MongoTemplateManager.closeApplicationContextIfOffline();
    }
}
