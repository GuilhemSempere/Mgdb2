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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;

import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;

/**
 * The Interface IExportHandler.
 */
public interface IExportHandler
{
	
	/** The Constant LOG. */
	static final Logger LOG = Logger.getLogger(IExportHandler.class);
	
	static final Document projectionDoc = new Document(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE, 1);	
	static final Document sortDoc = new Document(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE, 1);
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
	 * @return the export files' extensions.
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
	
	public static MongoCursor<Document> getMarkerCursorWithCorrectCollation(MongoCollection<Document> varColl, Document varQuery, int nQueryChunkSize) {
		return varColl.find(varQuery).projection(projectionDoc).sort(sortDoc).noCursorTimeout(true).collation(collationObj).batchSize(nQueryChunkSize).iterator();
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
	
	public static String getLoggedUserName() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth == null || "anonymousUser".equals(auth.getName()) ? "anonymousUser" : auth.getName();	    
	}
	
	public static void writeMetadataFile(String sModule, Collection<String> exportedIndividuals, Collection<String> individualMetadataFieldsToExport, OutputStream os) throws IOException {
        Collection<Individual> listInd = MgdbDao.getInstance().loadIndividualsWithAllMetadata(sModule, getLoggedUserName(), null, exportedIndividuals).values();
        LinkedHashSet<String> mdHeaders = new LinkedHashSet<>();	// definite header collection (avoids empty columns)
        for (Individual ind : listInd)
        	for (String key : individualMetadataFieldsToExport)
        		if (!mdHeaders.contains(key) && !Helper.isNullOrEmptyString(ind.getAdditionalInfo().get(key)))
        			mdHeaders.add(key);
        for (String headerKey : mdHeaders)
        	os.write(("\t" + headerKey).getBytes());
        os.write("\n".getBytes());
        
        for (Individual ind : listInd) {
        	 os.write(ind.getId().getBytes());
            for (String headerKey : mdHeaders)
            	os.write(("\t" + Helper.nullToEmptyString(ind.getAdditionalInfo().get(headerKey))).getBytes());
            os.write("\n".getBytes());
        }
	}

	/**
	 * Used for providing access to session attributes when exporting to server (in that case, export runs in a Thread and HttpResponse is returned immediately).
	 * We don't want to keep a reference to the session itself because it may get invalidated before the thread actually tries to access the attributes.
	 */
	public class SessionAttributeAwareExportThread extends Thread {
		private Map<String, Object> sessionAttributes = new HashMap<>();
		
		public SessionAttributeAwareExportThread(HttpSession session) {
	       Enumeration<String> attrNames = session.getAttributeNames();
	       while (attrNames.hasMoreElements() ) {
	            String name = attrNames.nextElement();
	            sessionAttributes.put(name, session.getAttribute(name));
	        }
		}
		
		public Map<String, Object> getSessionAttributes() {
			return sessionAttributes;
		}
	}
}