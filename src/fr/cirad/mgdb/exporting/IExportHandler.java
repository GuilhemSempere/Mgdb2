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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;

import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
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
	static final int nMaxChunkSizeInMb = 2;
	
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
	
	public static boolean addMetadataEntryIfAny(String fileName, String sModule, String sExportingUser, Collection<String> exportedIndividuals, Collection<String> individualMetadataFieldsToExport, ZipOutputStream zos, String initialContents) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (!writeMetadataFile(sModule, sExportingUser, exportedIndividuals, individualMetadataFieldsToExport, baos))
        	return false;

    	zos.putNextEntry(new ZipEntry(fileName));
    	if (initialContents != null)
    		zos.write(initialContents.getBytes());
    	byte[] byteArray = baos.toByteArray();
    	zos.write(byteArray, 0, byteArray.length);
    	zos.closeEntry();
    	return true;
	}
	
	public static MongoCursor<Document> getMarkerCursorWithCorrectCollation(MongoCollection<Document> varColl, Document varQuery, Document projectionAndSortDoc, int nQueryChunkSize) {
		return varColl.find(varQuery).projection(projectionAndSortDoc).sort(projectionAndSortDoc).noCursorTimeout(true).collation(collationObj).batchSize(nQueryChunkSize).iterator();
	}

	public static ZipOutputStream createArchiveOutputStream(OutputStream outputStream, Map<String, InputStream> readyToExportFiles) throws IOException {
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
        return zos;
	}
	
	public static int computeQueryChunkSize(MongoTemplate mongoTemplate, long nExportedVariantCount) {
		Number avgObjSize = (Number) mongoTemplate.getDb().runCommand(new Document("collStats", mongoTemplate.getCollectionName(VariantRunData.class))).get("avgObjSize");
		return (int) Math.max(1, Math.min(nExportedVariantCount / 20 /* no more than 5% at a time */, (nMaxChunkSizeInMb*1024*1024 / avgObjSize.doubleValue())));
	}
	
	public static boolean writeMetadataFile(String sModule, String sExportingUser, Collection<String> exportedIndividuals, Collection<String> individualMetadataFieldsToExport, OutputStream os) throws IOException {
		Collection<Individual> listInd = MgdbDao.getInstance().loadIndividualsWithAllMetadata(sModule, sExportingUser, null, exportedIndividuals, null).values();
        LinkedHashSet<String> mdHeaders = new LinkedHashSet<>();	// definite header collection (avoids empty columns)
        for (Individual ind : listInd) {
        	LinkedHashMap<String, Object> ai = ind.getAdditionalInfo();
        	Collection<String> fieldsToAccountFor = individualMetadataFieldsToExport == null ? ai.keySet() : individualMetadataFieldsToExport;
        	for (String key : fieldsToAccountFor)
        		if (!Helper.isNullOrEmptyString(ai.get(key)))
        			mdHeaders.add(key);
        }

        for (String headerKey : mdHeaders)
        	os.write(("\t" + headerKey).getBytes());
        os.write("\n".getBytes());
        
        for (Individual ind : listInd) {
        	 os.write(ind.getId().getBytes());
            for (String headerKey : mdHeaders)
            	os.write(("\t" + Helper.nullToEmptyString(ind.getAdditionalInfo().get(headerKey))).getBytes());
            os.write("\n".getBytes());
        }
        return !mdHeaders.isEmpty();
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
    
    public static void writeWarnings(ZipOutputStream zos, File[] warningFiles, String exportName) throws IOException {
    	try {
	        int nWarningCount = 0;
	        for (File f : warningFiles) {
		    	if (f.length() > 0) {
		            BufferedReader in = new BufferedReader(new FileReader(f));
		            String sLine;
		            while ((sLine = in.readLine()) != null) {
		            	if (nWarningCount == 0)
		                    zos.putNextEntry(new ZipEntry(exportName + "-REMARKS.txt"));
		                zos.write((sLine + "\n").getBytes());
		                nWarningCount++;
		            }
		            in.close();
		    	}
		    	f.delete();
	        }
	        if (nWarningCount > 0) {
		        LOG.info("Number of warnings for export (" + exportName + "): " + nWarningCount);
		        zos.closeEntry();
	        }
    	}
    	finally {
    		for (File f : warningFiles)
    			f.delete();
    	}
	}
}