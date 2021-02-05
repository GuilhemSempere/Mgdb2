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
package fr.cirad.mgdb.exporting.markeroriented;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import com.mongodb.client.MongoCollection;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.tools.ProgressIndicator;

/**
 * The Class AbstractMarkerOrientedExportHandler.
 */
public abstract class AbstractMarkerOrientedExportHandler implements IExportHandler
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(AbstractMarkerOrientedExportHandler.class);
	
	/** The marker oriented export handlers. */
	static private TreeMap<String, AbstractMarkerOrientedExportHandler> markerOrientedExportHandlers = null;

	protected static Document projectionDoc(Integer nAssemblyId) {
		return new Document(VariantData.FIELDNAME_REFERENCE_POSITION + (nAssemblyId != null ? "." + nAssemblyId : "") + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(VariantData.FIELDNAME_REFERENCE_POSITION + (nAssemblyId != null ? "." + nAssemblyId : "")  + "." + ReferencePosition.FIELDNAME_START_SITE, 1);	
	}

	protected static Document sortDoc(Integer nAssemblyId) {
		return new Document(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + (nAssemblyId != null ? "." + nAssemblyId : "")  + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(AbstractVariantData.FIELDNAME_REFERENCE_POSITION + (nAssemblyId != null ? "." + nAssemblyId : "")  + "." + ReferencePosition.FIELDNAME_START_SITE, 1);
	}

	/**
	 * Export data.
	 *
	 * @param outputStream the output stream
	 * @param sModule the module
	 * @param nAssemblyId ID of the assembly to work with
	 * @param sampleIDs1 the sample ids for group 1
	 * @param sampleIDs2 the sample ids for group 2
	 * @param progress the progress
	 * @param varColl the variant collection (main or temp)
	 * @param varQuery query to apply on varColl
	 * @param markerSynonyms the marker synonyms
	 * @param nMinimumReadDepth the n minimum read depth
	 * @param annotationFieldThresholds the annotation field thresholds for group 1
	 * @param annotationFieldThresholds2 the annotation field thresholds for group 2
	 * @param samplesToExport the samples to export genotyping data for
	 * @param readyToExportFiles files to export along with the genotyping data
	 * @throws Exception the exception
	 */
	abstract public void exportData(OutputStream outputStream, String sModule, Integer nAssemblyId,Collection<GenotypingSample> samples1, Collection<GenotypingSample> samples2, ProgressIndicator progress, MongoCollection<Document> varColl, Document varQuery, Map<String, String> markerSynonyms, HashMap<String, Float> annotationFieldThresholds, HashMap<String, Float> annotationFieldThresholds2, List<GenotypingSample> samplesToExport, Map<String, InputStream> readyToExportFiles) throws Exception;

//	/**
//	 * Gets the individuals from samples.
//	 *
//	 * @param sModule the module
//	 * @param sampleIDs the sample i ds
//	 * @return the individuals from samples
//	 */
//	protected List<Individual> getIndividualsFromSamples(final String sModule, final List<SampleId> sampleIDs)
//	{
//		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
//		HashMap<Integer, GenotypingProject> loadedProjects = new HashMap<Integer, GenotypingProject>();
//		ArrayList<Individual> result = new ArrayList<Individual>();
//		for (SampleId spId : sampleIDs)
//		{
//			GenotypingProject project = loadedProjects.get(spId.getProject());
//			if (project == null)
//			{
//				project = mongoTemplate.findById(spId.getProject(), GenotypingProject.class);
//				loadedProjects.put(spId.getProject(), project);
//			}
//			Integer spIndex = spId.getSampleIndex();
//			String individual = project.getSamples().get(spIndex).getIndividual();
//			result.add(mongoTemplate.findById(individual, Individual.class));
//		}
//		return result;
//	}
	
	/**
	 * Gets the marker oriented export handlers.
	 *
	 * @return the marker oriented export handlers
	 * @throws ClassNotFoundException the class not found exception
	 * @throws InstantiationException the instantiation exception
	 * @throws IllegalAccessException the illegal access exception
	 * @throws IllegalArgumentException the illegal argument exception
	 * @throws InvocationTargetException the invocation target exception
	 * @throws NoSuchMethodException the no such method exception
	 * @throws SecurityException the security exception
	 */
	public static TreeMap<String, AbstractMarkerOrientedExportHandler> getMarkerOrientedExportHandlers() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException
	{
		if (markerOrientedExportHandlers == null)
		{
			markerOrientedExportHandlers = new TreeMap<String, AbstractMarkerOrientedExportHandler>();
			ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
			provider.addIncludeFilter(new AssignableTypeFilter(AbstractMarkerOrientedExportHandler.class));
			try
			{
				for (BeanDefinition component : provider.findCandidateComponents("fr.cirad"))
				{
				    Class cls = Class.forName(component.getBeanClassName());
				    if (!Modifier.isAbstract(cls.getModifiers()))
				    {
						AbstractMarkerOrientedExportHandler exportHandler = (AbstractMarkerOrientedExportHandler) cls.getConstructor().newInstance();
						String sFormat = exportHandler.getExportFormatName();
						AbstractMarkerOrientedExportHandler previouslyFoundExportHandler = markerOrientedExportHandlers.get(sFormat);
						if (previouslyFoundExportHandler != null)
						{
							if (exportHandler.getClass().isAssignableFrom(previouslyFoundExportHandler.getClass()))
							{
								LOG.debug(previouslyFoundExportHandler.getClass().getName() + " implementation was preferred to " + exportHandler.getClass().getName() + " to handle exporting to '" + sFormat + "' format");
								continue;	// skip adding the current exportHandler because we already have a "better" one
							}
							else if (previouslyFoundExportHandler.getClass().isAssignableFrom(exportHandler.getClass()))
								LOG.debug(exportHandler.getClass().getName() + " implementation was preferred to " + previouslyFoundExportHandler.getClass().getName() + " to handle exporting to " + sFormat + "' format");
							else
								LOG.warn("Unable to choose between " + previouslyFoundExportHandler.getClass().getName() + " and " + exportHandler.getClass().getName() + ". Keeping first found: " + previouslyFoundExportHandler.getClass().getName());
						}
				    	markerOrientedExportHandlers.put(sFormat, exportHandler);
				    }
				}
			}
			catch (Exception e)
			{
				LOG.warn("Error scanning export handlers", e);
			}
		}
		return markerOrientedExportHandlers;
	}
	
	/* (non-Javadoc)
	 * @see fr.cirad.mgdb.exporting.IExportHandler#getSupportedVariantTypes()
	 */
	@Override
	public List<String> getSupportedVariantTypes()
	{
		return null;	// means any type
	}
	
	@Override
	public String getExportContentType() {
		return "application/zip";
	}
}