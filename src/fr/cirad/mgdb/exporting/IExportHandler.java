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
package fr.cirad.mgdb.exporting;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.ejb.ObjectNotFoundException;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;

import fr.cirad.mgdb.exporting.tools.ExportManager.ExportOutputs;
import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.Callset;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.AlphaNumericComparator;
import fr.cirad.tools.Helper;

/**
 * The Interface IExportHandler.
 */
public interface IExportHandler
{
	
	/** The Constant LOG. */
	static final Logger LOG = Logger.getLogger(IExportHandler.class);
	
	static final Collation collationObj = Collation.builder().numericOrdering(true).locale("en_US").build();
	
	/** The Constant nMaxChunkSizeInMb. */
	static final int nMaxChunkSizeInMb = 2;	// many different values were tested, this really looks like the best compromise
	
	/** The Constant LINE_SEPARATOR. */
	static final String LINE_SEPARATOR = "\n";
	
	/**
     * Gets the supported ploidy levels
     *
     * @return the export format name
     */
    public int[] getSupportedPloidyLevels();
	
	/**
	 * Gets the export format name.
	 *
	 * @return the export format name
	 */
	public String getExportFormatName();
	
	/**
	 * Gets the export format description.
	 *
	 * @return the export format description
	 */
	public String getExportFormatDescription();
	
	/**
	 * Gets the export archive extension.
	 *
	 * @return the export file extension.
	 */
	public String getExportArchiveExtension();
	
	/**
	 * Gets the export file content-type
	 *
	 * @return the export file content-type.
	 */
	public String getExportContentType();
	
	/**
	 * Gets the export files' extensions.
	 *
	 * @return the exp@Override
    ort files' extensions.
	 */
	public String[] getExportDataFileExtensions();
	
	/**
	 * Gets the step list.
	 *
	 * @return the step list
	 */
	public List<String> getStepList();
	
	/**
	 * Gets the supported variant types.
	 *
	 * @return the supported variant types
	 */
	public List<String> getSupportedVariantTypes();
	
	public void setTmpFolder(String tmpFolderPath);
	
	public String getMetadataContentsPrefix();
	
	public String getMetadataFileExtension();
	
	public static boolean addMetadataEntryIfAny(String fileName, String sModule, String sExportingUser, Collection<String> exportedIndividuals, Collection<String> metadataFieldsToExport, ZipOutputStream zos, String initialContents, boolean workWithSamples) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (!writeMetadataFile(sModule, sExportingUser, exportedIndividuals, metadataFieldsToExport, baos, workWithSamples, initialContents))
        	return false;

    	zos.putNextEntry(new ZipEntry(fileName));
    	byte[] byteArray = baos.toByteArray();
    	zos.write(byteArray, 0, byteArray.length);
    	zos.closeEntry();
    	return true;
	}
	
	public static MongoCursor<Document> getMarkerCursorWithCorrectCollation(MongoCollection<Document> varColl, Document varQuery, Document projectionAndSortDoc, int nQueryChunkSize) {
		return varColl.find(varQuery).projection(projectionAndSortDoc).sort(projectionAndSortDoc).noCursorTimeout(true).collation(collationObj).batchSize(nQueryChunkSize).iterator();
	}

	public static ZipOutputStream createArchiveOutputStream(OutputStream outputStream, Map<String, InputStream> readyToExportFiles, ExportOutputs exportOutputs) throws IOException {
        ZipOutputStream zos = new ZipOutputStream(outputStream);

        if (readyToExportFiles != null) {
            for (String readyToExportFile : readyToExportFiles.keySet()) {
                zos.putNextEntry(new ZipEntry(readyToExportFile));
                InputStream inputStream = readyToExportFiles.get(readyToExportFile);
                byte[] dataBlock = new byte[1024];
                int count = inputStream.read(dataBlock, 0, 1024);
                while (count != -1) {
                    zos.write(dataBlock, 0, count);
                    count = inputStream.read(dataBlock, 0, 1024);
                }
                zos.closeEntry();
            }
        }
        
        if (exportOutputs != null && exportOutputs.getMetadataFileContents() != null && !exportOutputs.getMetadataFileContents().isEmpty()) {
        	zos.putNextEntry(new ZipEntry(exportOutputs.getMetadataFileName()));
        	zos.write(exportOutputs.getMetadataFileContents().getBytes());
        	zos.closeEntry();
        }

        return zos;
	}
	
	public static int computeQueryChunkSize(MongoTemplate mongoTemplate, long nExportedVariantCount) {
		Number avgObjSize = (Number) mongoTemplate.getDb().runCommand(new Document("collStats", mongoTemplate.getCollectionName(VariantRunData.class))).get("avgObjSize");
		return (int) Math.max(1, Math.min(nExportedVariantCount / 20 /* no more than 5% at a time */, (nMaxChunkSizeInMb*1024*1024 / avgObjSize.doubleValue())));
	}
	
	public static boolean writeMetadataFile(String sModule, String sExportingUser, Collection<String> exportedIndividuals, Collection<String> individualMetadataFieldsToExport, OutputStream os, boolean workWithSamples, String initialContents) throws IOException {
        String contents = buildMetadataFile(sModule, sExportingUser, exportedIndividuals, individualMetadataFieldsToExport, workWithSamples, initialContents);
        if (contents.length() == initialContents.length())
        	return false;

        os.write(contents.getBytes());
        return true;
	}
	
	public static String buildMetadataFile(String sModule, String sExportingUser, Collection<String> exportedIndividuals, Collection<String> individualMetadataFieldsToExport, boolean workWithSamples, String initialContents) throws IOException {
		StringBuffer sb = new StringBuffer(initialContents);
		Collection material = workWithSamples ? MgdbDao.getInstance().loadSamplesWithAllMetadata(sModule, sExportingUser, null, exportedIndividuals, null, true).values() : MgdbDao.getInstance().loadIndividualsWithAllMetadata(sModule, sExportingUser, null, exportedIndividuals, null).values();
        LinkedHashSet<String> mdHeaders = new LinkedHashSet<>();	// definite header collection (avoids empty columns)
        for (Object indOrSp : material) {
        	LinkedHashMap<String, Object> ai = indOrSp instanceof Individual ? ((Individual) indOrSp).getAdditionalInfo() : ((GenotypingSample) indOrSp).getAdditionalInfo();
        	Collection<String> fieldsToAccountFor = individualMetadataFieldsToExport == null ? ai.keySet() : individualMetadataFieldsToExport;
        	for (String key : fieldsToAccountFor)
        		if (!Helper.isNullOrEmptyString(ai.get(key)))
        			mdHeaders.add(key);
        }

        for (String headerKey : mdHeaders)
        	sb.append(("\t" + headerKey));
        sb.append("\n");
        
        for (Object indOrSp : material) {
        	sb.append((indOrSp instanceof Individual ? ((Individual) indOrSp).getId() : ((GenotypingSample) indOrSp).getId().toString()));
        	LinkedHashMap<String, Object> ai = indOrSp instanceof Individual ? ((Individual) indOrSp).getAdditionalInfo() : ((GenotypingSample) indOrSp).getAdditionalInfo();
            for (String headerKey : mdHeaders)
            	sb.append(("\t" + Helper.nullToEmptyString(ai.get(headerKey))));
            sb.append("\n");
        }
        return sb.toString();
	}
	
    public static Map<String, String> getIndividualPopulations(final Map<String, Collection<String>> individualsByPopulation, boolean fAllowIndividualsInMultipleGroups) throws Exception {
    	Map<String, String> result = new HashMap<>();
    	for (String pop : individualsByPopulation.keySet())
    		for (String ind : individualsByPopulation.get(pop)) {
    			if (result.containsKey(ind)) {
    				if (!fAllowIndividualsInMultipleGroups)
    					throw new Exception("Individual " + ind + " is part of several groups!");
    				result.put(ind, result.get(ind) + ";" + pop);
    			}
    			else
    				result.put(ind, pop);
    		}
        return result;
    }
    
    public static void writeZipEntryFromChunkFiles(ZipOutputStream zos, File[] chunkFiles, String sZipEntryFileName) throws IOException {
    	writeZipEntryFromChunkFiles(zos, chunkFiles, sZipEntryFileName, null);
    }

    public static void writeZipEntryFromChunkFiles(ZipOutputStream zos, File[] chunkFiles, String sZipEntryFileName, String headerLine) throws IOException {
    	try {
	        int chunkCount = 0;
	        for (File f : chunkFiles) {
		    	if (f != null && f.length() > 0) {
			    	try (BufferedReader in = new BufferedReader(new FileReader(f))) {
			            String sLine;
			            while ((sLine = in.readLine()) != null) {
			            	if (chunkCount == 0) {
			                    zos.putNextEntry(new ZipEntry(sZipEntryFileName));
			                    if (headerLine != null)
			                    	zos.write((headerLine + "\n").getBytes());
			            	}
			            	zos.write((sLine + "\n").getBytes());
			                chunkCount++;
			            }
			            in.close();
			    	}
			    	f.delete();
		    	}
	        }
	        if (chunkCount > 0) {
		        LOG.debug("Number of entries in export file " + sZipEntryFileName + ": " + chunkCount);
		        zos.closeEntry();
	        }
    	}
    	finally {
    		for (File f : chunkFiles)
    			if (f != null)
    				f.delete();
    	}
	}
    
    public static Map<String, Integer> buildIndividualPositions(Collection<Callset> callSetsToExport, boolean workWithSamples) throws ObjectNotFoundException {
    	TreeSet<String> sortedIndividuals = new TreeSet<>(new AlphaNumericComparator<String>());
		for (Callset cs : callSetsToExport)
			sortedIndividuals.add(workWithSamples ? cs.getSampleId() : cs.getIndividual());			

		Map<String, Integer> individualPositions = new LinkedHashMap<>();
		for (String spOrInd : sortedIndividuals)
			individualPositions.put(spOrInd, individualPositions.size());
		return individualPositions;
    }

	public static String buildExportName(String sModule, Assembly assembly, long markerCount, int indOrSampleCount, boolean workWithSamples) {
		return sModule + (assembly != null && assembly.getName() != null ? "__" + assembly.getName() : "") + "__" + markerCount + "variants__" + indOrSampleCount + (workWithSamples ? "sample" : "individual" ) + "s";
	}
}