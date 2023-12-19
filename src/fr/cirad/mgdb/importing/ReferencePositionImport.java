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
import java.io.FileReader;
import java.text.NumberFormat;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.client.result.UpdateResult;

import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class ReferencePositionImport.
 */
public class ReferencePositionImport {
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(ReferencePositionImport.class);
	
	/** The Constant twoDecimalNF. */
	static private final NumberFormat twoDecimalNF = NumberFormat.getInstance();

	static
	{
		twoDecimalNF.setMaximumFractionDigits(2);
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception
	{
		insertReferencePositions(args);
	}

	/**
	 * Insert reference positions.
	 *
	 * @param args the args
	 * @throws Exception the exception
	 */
	public static void insertReferencePositions(String[] args) throws Exception
	{
		if (args.length < 5)
			throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, chip info folder, marker name column number, marker chromosome column number, marker position column number. Chip info folder is meant to contain one or several csv files (supported separators are coma, space and tab) providing marker names, chromosomes and positions.");

		File chipInfoFolder = new File(args[1]);
		if (!chipInfoFolder.exists() || !chipInfoFolder.isDirectory())
			throw new Exception("Folder does not exist: " + chipInfoFolder.getAbsolutePath());

		File[] chipInfoFiles = chipInfoFolder.listFiles();
		if (chipInfoFiles.length == 0)
			throw new Exception("No chip info files found in folder: " + chipInfoFolder.getAbsolutePath());
		
		int nMarkerNameColNum, nMarkerChrColNum, nMarkerPosColNum;
		try
		{
			nMarkerNameColNum = Integer.parseInt(args[2]);
		}
		catch (NumberFormatException nfe)
		{
			throw new Exception("Unable to parse marker name column number: " + args[2]);
		}
		try
		{
			nMarkerChrColNum = Integer.parseInt(args[3]);
		}
		catch (NumberFormatException nfe)
		{
			throw new Exception("Unable to parse marker chromosome column number: " + args[3]);
		}
		try
		{
			nMarkerPosColNum = Integer.parseInt(args[4]);
		}
		catch (NumberFormatException nfe)
		{
			throw new Exception("Unable to parse marker position column number: " + args[4]);
		}

		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(args[0]);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(args[0]);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + args[0] + "' is not supported!");
			}

			for (int i=0; i<chipInfoFiles.length; i++)
			{
				int nVariantIndex = 0;
				if (chipInfoFiles[i].isDirectory())
					continue;
				
				BufferedReader in = new BufferedReader(new FileReader(chipInfoFiles[i]));
				try
				{
					in.readLine();	// skip header
					String sLine = in.readLine();
					if (sLine != null)
						sLine = sLine.trim();
					do
					{
						if (sLine.length() > 0)
						{
							nVariantIndex++;
							List<String> cells = splitByComaSpaceOrTab(sLine);
		
							Boolean fSaved = null;
							for (int j=0; j<10; j++)
							{
								Query q = new Query(Criteria.where("_id").is(cells.get(nMarkerNameColNum)));
								q.fields().include(VariantData.FIELDNAME_VERSION);
								q.fields().include(VariantData.FIELDNAME_REFERENCE_POSITION);
								VariantData variant = mongoTemplate.findOne(q, VariantData.class);
								if (variant == null)
								{
									LOG.warn("Not found: " + cells.get(nMarkerNameColNum) + " (" + nVariantIndex + ")");
									fSaved = false;
									break;
								}
								else
								{
									ReferencePosition chromPos = new ReferencePosition(cells.get(nMarkerChrColNum), Integer.parseInt(cells.get(nMarkerPosColNum)));
									if (chromPos.equals(variant.getReferencePosition(0 /*FIXME : make it multi-assembly?*/)))
									{
										LOG.warn("No change to apply: " + cells.get(nMarkerNameColNum) + " (" + nVariantIndex + ")");
										fSaved = false;
										break;
									}
									q = new Query(Criteria.where("_id").is(cells.get(nMarkerNameColNum))).addCriteria(Criteria.where(VariantData.FIELDNAME_VERSION).is(variant.getVersion()));
									try
									{
										UpdateResult ur = mongoTemplate.updateFirst(q, new Update().set(VariantData.FIELDNAME_REFERENCE_POSITION, chromPos), VariantData.class);
										if (ur.getModifiedCount() == 0)
											throw new Exception("Not written: " + cells.get(nMarkerNameColNum) + " (" + nVariantIndex + ")");
										else if (nVariantIndex % 5000 == 0)
											LOG.info(nVariantIndex + " variants processed for file n." + (i+1));
										fSaved = true;
										if (j > 0)
											LOG.warn("Took " + j + " retries to save variant " + cells.get(nMarkerNameColNum));
										break;
									}
									catch (OptimisticLockingFailureException olfe)
									{
										LOG.error("failed: " + cells.get(nMarkerNameColNum));
									}
								}
							}
							if (fSaved == null)
								LOG.error("Could not be saved because of concurrent writing: " + cells.get(nMarkerNameColNum) + " (" + nVariantIndex + ")");
						}
						sLine = in.readLine();
						if (sLine != null)
							sLine = sLine.trim();
					}
					while (sLine != null);		
				}
				finally
				{
					in.close();			
				}
			}
		}
		finally
		{
			if (ctx != null)
				ctx.close();
		}
	}

	/**
	 * Split by coma space or tab.
	 *
	 * @param s the s
	 * @return the list
	 */
	private static List<String> splitByComaSpaceOrTab(String s)
	{
		return Helper.split(s, s.contains(",") ? "," : (s.contains(" ") ? " " : "\t"));
	}
}
