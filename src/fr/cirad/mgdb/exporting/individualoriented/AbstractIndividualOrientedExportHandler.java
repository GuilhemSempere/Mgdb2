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
package fr.cirad.mgdb.exporting.individualoriented;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.DBCursor;

import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.exporting.tools.AsyncExportTool;
import fr.cirad.mgdb.exporting.tools.AsyncExportTool.AbstractDataOutputHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.AlphaNumericComparator;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class AbstractIndividualOrientedExportHandler.
 */
public abstract class AbstractIndividualOrientedExportHandler implements IExportHandler
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(AbstractIndividualOrientedExportHandler.class);
	
	/** The individual oriented export handlers. */
	static private TreeMap<String, AbstractIndividualOrientedExportHandler> individualOrientedExportHandlers = null;
	
	/**
	 * Export data.
	 *
	 * @param outputStream the output stream
	 * @param sModule the module
	 * @param individualExportFiles the individual export files
	 * @param fDeleteSampleExportFilesOnExit whether or not to delete sample export files on exit
	 * @param progress the progress
	 * @param markerCursor the marker cursor
	 * @param markerSynonyms the marker synonyms
	 * @param readyToExportFiles the ready to export files
	 * @throws Exception the exception
	 */
	abstract public void exportData(OutputStream outputStream, String sModule, Collection<File> individualExportFiles, boolean fDeleteSampleExportFilesOnExit, ProgressIndicator progress, DBCursor markerCursor, Map<String, String> markerSynonyms, Map<String, InputStream> readyToExportFiles) throws Exception;

	/**
	 * Creates the export files.
	 *
	 * @param sModule the module
	 * @param markerCursor the marker cursor
	 * @param samples1 the samples for group 1
	 * @param samples2 the samples for group 2
	 * @param exportID the export id
	 * @param annotationFieldThresholds the annotation field thresholds for group 1
	 * @param annotationFieldThresholds2 the annotation field thresholds for group 2
	 * @param sampleToIndividualMapToExport 
	 * @param progress the progress
	 * @return a map providing one File per individual
	 * @throws Exception the exception
	 */
	public TreeMap<String, File> createExportFiles(String sModule, DBCursor markerCursor, Collection<GenotypingSample> samples1, Collection<GenotypingSample> samples2, String exportID, HashMap<String, Float> annotationFieldThresholds, HashMap<String, Float> annotationFieldThresholds2, List<GenotypingSample> samplesToExport, final ProgressIndicator progress) throws Exception
	{
		long before = System.currentTimeMillis();

		List<String> individuals1 = MgdbDao.getIndividualsFromSamples(sModule, samples1).stream().map(ind -> ind.getId()).collect(Collectors.toList());	
		List<String> individuals2 = MgdbDao.getIndividualsFromSamples(sModule, samples2).stream().map(ind -> ind.getId()).collect(Collectors.toList());

		TreeMap<String, File> files = new TreeMap<String, File>(new AlphaNumericComparator<String>());
		int i = 0;
		for (GenotypingSample sample : samplesToExport)
		{
			String individual = sample.getIndividual();
			if (!files.containsKey(individual))
			{
				File file = File.createTempFile(exportID.replaceAll("\\|", "&curren;") +  "-" + individual + "-", ".tsv");
				files.put(individual, file);
				if (i == 0)
					LOG.debug("First temp file for export " + exportID + ": " + file.getPath());
				files.put(individual, file);
				BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
				os.write((individual + LINE_SEPARATOR).getBytes());
				os.close();
				i++;
			}
		}
		ArrayList<String> sortedIndList = new ArrayList(files.keySet());
		
		final MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);

		final Map<Integer, String> sampleIdToIndividualMap = new HashMap<>();
		for (GenotypingSample gs : samplesToExport)
			sampleIdToIndividualMap.put(gs.getId(), gs.getIndividual());

    	Number avgObjSize = (Number) mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantRunData.class)).getStats().get("avgObjSize");
		int nQueryChunkSize = (int) Math.max(1, (nMaxChunkSizeInMb*1024*1024 / avgObjSize.doubleValue()) / AsyncExportTool.INITIAL_NUMBER_OF_SIMULTANEOUS_QUERY_THREADS);

		AtomicLong nLoadedMarkerCount = new AtomicLong(0);
		int markerCount = markerCursor.count();

		AbstractDataOutputHandler<Integer, LinkedHashMap<VariantData, Collection<VariantRunData>>> dataOutputHandler = new AbstractDataOutputHandler<Integer, LinkedHashMap<VariantData, Collection<VariantRunData>>>() {				
			@Override
			public Void call() {
				try
				{
					StringBuffer[] individualGenotypeBuffers = new StringBuffer[sortedIndList.size()];	// keeping all files open leads to failure (see ulimit command), keeping them closed and reopening them each time we need to write a genotype is too time consuming: so our compromise is to reopen them only once per chunk
					for (VariantData variant : variantDataChunkMap.keySet())
					{
						if (progress.isAborted())
							break;
						
						HashMap<String, List<String>> individualGenotypes = new HashMap<String, List<String>>();
						
						Collection<VariantRunData> runs = variantDataChunkMap.get(variant);
						if (runs != null)
							for (VariantRunData run : runs)
								for (Integer sampleId : run.getSampleGenotypes().keySet())
								{
									SampleGenotype sampleGenotype = run.getSampleGenotypes().get(sampleId);
									List<String> alleles = variant.getAllelesFromGenotypeCode(sampleGenotype.getCode());
									String individualId = sampleIdToIndividualMap.get(sampleId);

									if (!VariantData.gtPassesVcfAnnotationFilters(individualId, sampleGenotype, individuals1, annotationFieldThresholds, individuals2, annotationFieldThresholds2))
										continue;	// skip genotype

									List<String> storedIndividualGenotypes = individualGenotypes.get(individualId);
									if (storedIndividualGenotypes == null)
									{
										storedIndividualGenotypes = new ArrayList<String>();
										individualGenotypes.put(individualId, storedIndividualGenotypes);
									}

									String sAlleles = StringUtils.join(alleles, ' ');
									storedIndividualGenotypes.add(sAlleles);
								}

						for (int i=0; i<sortedIndList.size(); i++)
						{
							String individual = sortedIndList.get(i);
							if (individualGenotypeBuffers[i] == null)
								individualGenotypeBuffers[i] = new StringBuffer();	// we are about to write individual's first genotype for this chunk

							List<String> storedIndividualGenotypes = individualGenotypes.get(individual);
							if (storedIndividualGenotypes == null)
								individualGenotypeBuffers[i].append(LINE_SEPARATOR);	// missing data
							else
								for (int j=0; j<storedIndividualGenotypes.size(); j++)
								{
									String storedIndividualGenotype = storedIndividualGenotypes.get(j);
									individualGenotypeBuffers[i].append(storedIndividualGenotype + (j == storedIndividualGenotypes.size() - 1 ? LINE_SEPARATOR : "|"));
								}
						}
						nLoadedMarkerCount.incrementAndGet();
					}

					// write genotypes collected in this chunk to each individual's file
					for (int i=0; i<sortedIndList.size(); i++)
					{
						String individual = sortedIndList.get(i);
						BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(files.get(individual), true));
						if (individualGenotypeBuffers[i] != null)
							os.write(individualGenotypeBuffers[i].toString().getBytes());

						os.close();
					}
	            }
				catch (Exception e)
				{
					if (progress.getError() == null)	// only log this once
						LOG.debug("Error creating temp files for export", e);
					progress.setError("Error creating temp files for export: " + e.getMessage());
				}
				return null;
			}
		};
		
		AsyncExportTool syncExportTool = new AsyncExportTool(markerCursor, markerCount, nQueryChunkSize, mongoTemplate, samplesToExport, dataOutputHandler, progress);
		syncExportTool.launch();

		while (progress.getCurrentStepProgress() < 100 && !progress.isAborted())
			Thread.sleep(500);

	 	if (!progress.isAborted())
	 		LOG.info("createExportFiles took " + (System.currentTimeMillis() - before)/1000d + "s to process " + markerCount + " variants and " + files.size() + " individuals");
		
		return files;
	}
	
	/**
	 * Gets the individual oriented export handlers.
	 *
	 * @return the individual oriented export handlers
	 * @throws ClassNotFoundException the class not found exception
	 * @throws InstantiationException the instantiation exception
	 * @throws IllegalAccessException the illegal access exception
	 * @throws IllegalArgumentException the illegal argument exception
	 * @throws InvocationTargetException the invocation target exception
	 * @throws NoSuchMethodException the no such method exception
	 * @throws SecurityException the security exception
	 */
	public static TreeMap<String, AbstractIndividualOrientedExportHandler> getIndividualOrientedExportHandlers() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException
	{
		if (individualOrientedExportHandlers == null)
		{
			individualOrientedExportHandlers = new TreeMap<String, AbstractIndividualOrientedExportHandler>();
			ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
			provider.addIncludeFilter(new AssignableTypeFilter(AbstractIndividualOrientedExportHandler.class));
			try
			{
				for (BeanDefinition component : provider.findCandidateComponents("fr.cirad"))
				{
				    Class cls = Class.forName(component.getBeanClassName());
				    if (!Modifier.isAbstract(cls.getModifiers()))
				    {
						AbstractIndividualOrientedExportHandler exportHandler = (AbstractIndividualOrientedExportHandler) cls.getConstructor().newInstance();
						String sFormat = exportHandler.getExportFormatName();
						AbstractIndividualOrientedExportHandler previouslyFoundExportHandler = individualOrientedExportHandlers.get(sFormat);
						if (previouslyFoundExportHandler != null)
						{
							if (exportHandler.getClass().isAssignableFrom(previouslyFoundExportHandler.getClass()))
							{
								LOG.debug(previouslyFoundExportHandler.getClass().getName() + " implementation was preferred to " + exportHandler.getClass().getName() + " to handle exporting to '" + sFormat + " format");
								continue;	// skip adding the current exportHandler because we already have a "better" one
							}
							else if (previouslyFoundExportHandler.getClass().isAssignableFrom(exportHandler.getClass()))
								LOG.debug(exportHandler.getClass().getName() + " implementation was preferred to " + previouslyFoundExportHandler.getClass().getName() + " to handle exporting to " + sFormat + " format");
							else
								LOG.warn("Unable to choose between " + previouslyFoundExportHandler.getClass().getName() + " and " + exportHandler.getClass().getName() + ". Keeping first found: " + previouslyFoundExportHandler.getClass().getName());
						}
				    	individualOrientedExportHandlers.put(sFormat, exportHandler);
				    }
				}
			}
			catch (Exception e)
			{
				LOG.warn("Error scanning export handlers", e);
			}
		}
		return individualOrientedExportHandlers;
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
