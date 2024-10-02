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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mgdb.VariantQueryWrapper;

/**
 * The Class AbstractMarkerOrientedExportHandler.
 */
public abstract class AbstractMarkerOrientedExportHandler implements IExportHandler
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(AbstractMarkerOrientedExportHandler.class);
	
	/** The marker oriented export handlers. */
	static private TreeMap<String, AbstractMarkerOrientedExportHandler> markerOrientedExportHandlers = null;

	protected String tmpFolderPath;

    /**
     * Export data.
     *
     * @param outputStream the output stream
     * @param sModule the module
     * @param nAssemblyId ID of the assembly to work with
	 * @param sExportingUser the user who launched the export
     * @param progress the progress
     * @param tmpVarCollName the variant collection name (null if not temporary)
     * @param varQueryWrapper variant query wrapper
     * @param markerCount number of variants to export
     * @param markerSynonyms the marker synonyms
     * @param individuals List of the individuals in each group
     * @param annotationFieldThresholds the annotation field thresholds for each group
     * @param samplesToExport the samples to export genotyping data for
     * @param individualMetadataFieldsToExport metadata fields to export for individuals
     * @param readyToExportFiles files to export along with the genotyping data
     * @throws Exception the exception
     */
    abstract public void exportData(OutputStream outputStream, String sModule, Integer nAssemblyId, String sExportingUser, ProgressIndicator progress, String tmpVarCollName, VariantQueryWrapper varQueryWrapper, long markerCount, Map<String, String> markerSynonyms, Map<String, Collection<String>> individuals, Map<String, HashMap<String, Float>> annotationFieldThresholds, Collection<GenotypingSample> samplesToExport, Collection<String> individualMetadataFieldsToExport, Map<String, InputStream> readyToExportFiles) throws Exception;

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
	
	static public LinkedHashMap<Object, Integer> sortGenotypesFromMostFound(Collection<String> genotypes) throws IOException {
        // Create a LinkedHashMap to maintain the sorted order
        LinkedHashMap<Object, Integer> sortedMap = new LinkedHashMap<>();
		if (genotypes != null) {
	        Map<String, Integer> countMap = new HashMap<>();
	        
	        // Count occurrences of strings
	        for (String str : genotypes)
	            countMap.put(str, countMap.getOrDefault(str, 0) + 1);
	        
	        // Sort the map by values using a custom Comparator
	        List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(countMap.entrySet());
	        sortedEntries.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));
	        
	        for (Map.Entry<String, Integer> entry : sortedEntries)
	            sortedMap.put(entry.getKey(), entry.getValue());
		}
        return sortedMap;
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
	
	@Override
	public void setTmpFolder(String tmpFolderPath) {
		this.tmpFolderPath = tmpFolderPath;	
	}
}