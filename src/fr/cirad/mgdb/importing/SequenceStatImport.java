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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;

import fr.cirad.mgdb.model.mongo.maintypes.SequenceStats;
import fr.cirad.mgdb.model.mongo.subtypes.MappingStats;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class SequenceStatImport.
 */
public class SequenceStatImport {
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(SequenceStatImport.class);
	
	/** The Constant SEQUENCE_FIELD_NAME. */
	private static final String SEQUENCE_FIELD_NAME = "Contig";
	
	/** The Constant SEQUENCE_FIELD_SEQLENGTH. */
	private static final String SEQUENCE_FIELD_SEQLENGTH = "length";
	
	/** The Constant SEQUENCE_FIELD_MAPPINGLENGTH. */
	private static final String SEQUENCE_FIELD_MAPPINGLENGTH = "mapping length";
	
	/** The Constant SEQUENCE_FIELD_P4EMETHOD. */
	private static final String SEQUENCE_FIELD_P4EMETHOD = "p4e method";
	
	/** The Constant SEQUENCE_FIELD_CDSSTART. */
	private static final String SEQUENCE_FIELD_CDSSTART = "CDS start";
	
	/** The Constant SEQUENCE_FIELD_CDSEND. */
	private static final String SEQUENCE_FIELD_CDSEND = "CDS end";
	
	/** The Constant SEQUENCE_FIELD_FRAME. */
	private static final String SEQUENCE_FIELD_FRAME = "frame";
	
	/** The Constant SEQUENCE_FIELD_PREFIX_RPKM. */
	private static final String SEQUENCE_FIELD_PREFIX_RPKM = "RPKM ";
	
	/** The Constant MANDATORY_SEQUENCE_FIELDS. */
	private static final String[] MANDATORY_SEQUENCE_FIELDS = { SEQUENCE_FIELD_NAME, SEQUENCE_FIELD_SEQLENGTH};
	
	/** The abort if any sequence is invalid. */
	private static boolean ABORT_IF_ANY_SEQUENCE_IS_INVALID = true;
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 5)
			throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, project name, run name, TSV-stats file, 5th parameter only supports values '2' (empty all database's sequence stats before importing), and '0' (no action over existing data)!");
		
		String sModule = args[0];
		
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		
		String sequenceStatCollName = MongoTemplateManager.getMongoCollectionName(SequenceStats.class);
		
		if ("2".equals(args[4])) {	// empty project's sequence data before importing
			if (mongoTemplate.collectionExists(sequenceStatCollName)) {
				mongoTemplate.dropCollection(sequenceStatCollName);
				LOG.info("Collection " + sequenceStatCollName + " dropped.");
			}	
		}	
		else if ("0".equals(args[4])) {
			// do nothing
		}
		else
			throw new Exception("5th parameter only supports values '2' (empty all database's sequence stats before importing), and '0' (no action over existing data)");
		
		ArrayList<SequenceStats> sequences = new ArrayList<SequenceStats>();
		
		LOG.info("Reading stat file");
		BufferedReader mainFileReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[3]))));
		StringBuffer errors = new StringBuffer();
		String line;
		
		HashMap<String, Integer> fieldIndexes = new HashMap<String, Integer>();
		int rowIndex = 0;
		try {
			while ((line = mainFileReader.readLine()) != null /*&& rowIndex < 5*/) {
				String[] splittedLine = line.split("\t");
				if (rowIndex == 0) {
					for (int i=0; i<splittedLine.length; i++)
						fieldIndexes.put(splittedLine[i], i);
					for (String sFieldName : MANDATORY_SEQUENCE_FIELDS)
						if (!fieldIndexes.containsKey(sFieldName))
							errors.append("\nMandatory field '" + sFieldName + "' missing from file " + args[0]);
					if (errors.length() > 0)
						throw new Exception("ERRORS IN FILE STRUCTURE: \n" + errors.toString());
				}
				else {
					String name = splittedLine[fieldIndexes.get(SEQUENCE_FIELD_NAME)];
					Long sequenceLength = null, mappingLength = null, cdsStart = null, cdsEnd = null;
					Byte frame = null;
					try {
						sequenceLength = Long.parseLong(splittedLine[fieldIndexes.get(SEQUENCE_FIELD_SEQLENGTH)]);
					}
					catch (NumberFormatException nfe)
					{
						errors.append("Unable to parse sequence length '" + fieldIndexes.get(SEQUENCE_FIELD_SEQLENGTH) + "' for sequence '" + name + "'");
						continue;
					}
					try {
						mappingLength = Long.parseLong(ignoreSingleDot(splittedLine[fieldIndexes.get(SEQUENCE_FIELD_MAPPINGLENGTH)]));
					}
					catch (NumberFormatException ignored)
					{}
					try {
						cdsStart = Long.parseLong(ignoreSingleDot(splittedLine[fieldIndexes.get(SEQUENCE_FIELD_CDSSTART)]));
					}
					catch (NumberFormatException ignored)
					{}
					try {
						cdsEnd = Long.parseLong(ignoreSingleDot(splittedLine[fieldIndexes.get(SEQUENCE_FIELD_CDSEND)]));
					}
					catch (NumberFormatException ignored)
					{}
					try {
						frame = Byte.parseByte(ignoreSingleDot(splittedLine[fieldIndexes.get(SEQUENCE_FIELD_FRAME)]));
					}
					catch (NumberFormatException ignored)
					{}
				
					SequenceStats seqStats = mongoTemplate.findById(name, SequenceStats.class);
					String p4eMethod = ignoreSingleDot(splittedLine[fieldIndexes.get(SEQUENCE_FIELD_P4EMETHOD)]);
					if (seqStats == null)
						seqStats = new SequenceStats(name, sequenceLength);
					else {
						if (cdsStart != null && !cdsStart.equals(seqStats.getCdsStart()))
							LOG.warn("Sequence '" + name + "': Value provided for '" + SEQUENCE_FIELD_CDSSTART + "' does not match the existing one. Overwriting existing value '" + cdsStart + "'");
						if (cdsEnd != null && !cdsEnd.equals(seqStats.getCdsEnd()))
							LOG.warn("Sequence '" + name + "': Value provided for '" + SEQUENCE_FIELD_CDSEND + "' does not match the existing one. Overwriting existing value '" + cdsEnd + "'");
						if (frame != null && !frame.equals(seqStats.getFrame()))
							LOG.warn("Sequence '" + name + "': Value provided for '" + SEQUENCE_FIELD_FRAME + "' does not match the existing one. Overwriting existing value '" + frame + "'");
						if (p4eMethod.length() > 0 && !p4eMethod.equals(seqStats.getP4eMethod()))
							LOG.warn("Sequence '" + name + "': Value provided for '" + SEQUENCE_FIELD_P4EMETHOD + "' does not match the existing one. Overwriting existing value '" + p4eMethod + "'");
					}
					seqStats.setCdsStart(cdsStart);
					seqStats.setCdsEnd(cdsEnd);
					seqStats.setFrame(frame);
					seqStats.setP4eMethod(p4eMethod);
					
					HashMap<String, HashMap<String, Comparable>> sampleInfoMap = new HashMap<String, HashMap<String, Comparable>>();
					List<MappingStats> theProjectData = seqStats.getProjectData().get(args[1]);
					if (theProjectData == null) {
						theProjectData = new ArrayList<MappingStats>();
						seqStats.getProjectData().put(args[1], theProjectData);
					}					
					MappingStats theMappingStats = null;
					for (MappingStats mp : theProjectData)
						if (args[2].equals(mp.getRunName())) {
							theMappingStats = mp;
							theMappingStats.setSampleInfo(sampleInfoMap);
							break;
						}
					if (theMappingStats == null) {
						theMappingStats = new MappingStats(args[2], sampleInfoMap);
						theProjectData.add(theMappingStats);
					}
					if (mappingLength != null)
						theMappingStats.setMappingLength(mappingLength);

					for (String sKey : fieldIndexes.keySet())
						if (sKey.startsWith(SEQUENCE_FIELD_PREFIX_RPKM)) {
							String rpkmString = splittedLine[fieldIndexes.get(sKey)];
							Float rpkm = null;
							try
							{
								rpkm = Float.parseFloat(rpkmString);
							}
							catch (NumberFormatException nfe)
							{
								errors.append("\nUnable to parse rpkm value for sequence '" + name + "' in field " + sKey);
							}
						
							String sSampleName = sKey.split(" ")[1];							
							
							HashMap<String, Comparable> sampleStats = new HashMap<String, Comparable>();
							sampleStats.put(MappingStats.SAMPLE_INFO_FIELDNAME_RPKM, rpkmString /*store it as string because MongoDB does not store Floats accurately*/);
							sampleInfoMap.put(sSampleName, sampleStats);
						}
					LOG.debug("Setting average rpkm to " + seqStats.calculateAverageRpkm(true) + " for sequence '" + name + "'");
					sequences.add(seqStats);
				}
				if (++rowIndex%1000 == 0)
					System.out.print(rowIndex + " ");
			}
			
			if (ABORT_IF_ANY_SEQUENCE_IS_INVALID && errors.length() > 0)
				throw new Exception("ERRORS IN FILE CONTENTS: \n" + errors.toString());
			
			rowIndex = 0;
			System.out.println();
			LOG.info("Writing records to database");
			for (SequenceStats sequence : sequences) {
				mongoTemplate.save(sequence, sequenceStatCollName);
				if (++rowIndex%1000 == 0)
					System.out.print(rowIndex + " ");
			}
			System.out.println();
			LOG.info((rowIndex) + " records added to collection " + sequenceStatCollName);
		}
		finally {
			mainFileReader.close();
		}

		MongoTemplateManager.closeApplicationContextIfOffline();
	}

	/**
	 * Ignore single dot.
	 *
	 * @param s the s
	 * @return the string
	 */
	private static String ignoreSingleDot(String s) {
		return ".".equals(s) ? "" : s;
	}

}
