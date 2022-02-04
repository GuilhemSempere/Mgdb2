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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoCollection;

import fr.cirad.mgdb.exporting.AbstractExportWritingThread;
import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.exporting.tools.ExportManager;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
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
	 * @param individualsToExport names of individuals to export
	 * @param individualExportFiles the individual export files
	 * @param fDeleteSampleExportFilesOnExit whether or not to delete sample export files on exit
	 * @param progress the progress
	 * @param tmpVarCollName the variant collection name (null if not temporary)
	 * @param varQuery query to apply on varColl
	 * @param markerCount number of variants to export
	 * @param markerSynonyms the marker synonyms
	 * @param individualMetadataFieldsToExport metadata fields to export for individuals 
	 * @param readyToExportFiles the ready to export files
	 * @throws Exception the exception
	 */
	abstract public void exportData(OutputStream outputStream, String sModule, Collection<String> individualsToExport, File[] individualExportFiles, boolean fDeleteSampleExportFilesOnExit, ProgressIndicator progress, String tmpVarCollName, Document varQuery, long markerCount, Map<String, String> markerSynonyms, Collection<String> individualMetadataFieldsToExport, Map<String, InputStream> readyToExportFiles) throws Exception;

	/**
	 * Creates the export files.
	 *
	 * @param sModule the module
	 * @param projId the project ID
	 * @param tmpVarCollName the variant collection name (null if not temporary)
	 * @param varQuery query to apply on varColl
	 * @param markerCount number of variants to export
	 * @param individuals1 the individuals in group 1
	 * @param individuals2 the individuals in group 2
	 * @param exportID the export id
	 * @param annotationFieldThresholds the annotation field thresholds for group 1
	 * @param annotationFieldThresholds2 the annotation field thresholds for group 2
	 * @param samplesToExport 
	 * @param progress the progress
	 * @return a map providing one File per individual
	 * @throws Exception the exception
	 */
	public File[] createExportFiles(String sModule, String tmpVarCollName, Document varQuery, long markerCount, Collection<String> individuals1, Collection<String> individuals2, String exportID, HashMap<String, Float> annotationFieldThresholds, HashMap<String, Float> annotationFieldThresholds2, List<GenotypingSample> samplesToExport, final ProgressIndicator progress) throws Exception
	{
		long before = System.currentTimeMillis();

		Map<String, Integer> individualPositions = new LinkedHashMap<>();
		for (String ind : samplesToExport.stream().map(gs -> gs.getIndividual()).distinct().sorted(new AlphaNumericComparator<String>()).collect(Collectors.toList()))
			individualPositions.put(ind, individualPositions.size());
		
		File[] files = new File[individualPositions.size()];
		int i = 0;
		for (String individual : individualPositions.keySet()) {
			files[i] = File.createTempFile(exportID.replaceAll("\\|", "&curren;") +  "-" + individual + "-", ".tsv");
			if (i == 0)
				LOG.debug("First temp file for export " + exportID + ": " + files[i].getPath());
			BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(files[i++]));
			os.write((individual + LINE_SEPARATOR).getBytes());
			os.close();
		}

		final MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);

		final Map<Integer, String> sampleIdToIndividualMap = samplesToExport.stream().collect(Collectors.toMap(GenotypingSample::getId, sp -> sp.getIndividual()));
		final AtomicInteger initialStringBuilderCapacity = new AtomicInteger();

		int nQueryChunkSize = IExportHandler.computeQueryChunkSize(mongoTemplate, markerCount);
		MongoCollection collWithPojoCodec = mongoTemplate.getDb().withCodecRegistry(ExportManager.pojoCodecRegistry).getCollection(tmpVarCollName != null ? tmpVarCollName : mongoTemplate.getCollectionName(VariantRunData.class));

		AbstractExportWritingThread writingThread = new AbstractExportWritingThread() {
			public void run() {
				StringBuilder[] individualGenotypeBuffers = new StringBuilder[individualPositions.size()];	// keeping all files open leads to failure (see ulimit command), keeping them closed and reopening them each time we need to write a genotype is too time consuming: so our compromise is to reopen them only once per chunk
				try
				{
				    markerRunsToWrite.forEach(runsToWrite -> {
						if (progress.isAborted() || progress.getError() != null)
							return;

						HashMap<String, String> genotypeStringCache = new HashMap<>();
						LinkedHashSet<String>[] individualGenotypes = new LinkedHashSet[individualPositions.size()];
		                if (runsToWrite != null)
		                	for (Object vrd : runsToWrite) {
		                    	VariantRunData run = (VariantRunData) vrd;
								for (Integer sampleId : run.getSampleGenotypes().keySet()) {
	                                String individualId = sampleIdToIndividualMap.get(sampleId);
	                                Integer individualIndex = individualPositions.get(individualId);
	                                if (individualIndex == null)
	                                    continue;   // unwanted sample

									SampleGenotype sampleGenotype = run.getSampleGenotypes().get(sampleId);
									if (!VariantData.gtPassesVcfAnnotationFilters(individualId, sampleGenotype, individuals1, annotationFieldThresholds, individuals2, annotationFieldThresholds2))
										continue;	// skip genotype

				                    String exportedGT = genotypeStringCache.get(sampleGenotype.getCode());
				                    if (exportedGT == null) {
				                    	exportedGT = StringUtils.join(run.safelyGetAllelesFromGenotypeCode(sampleGenotype.getCode(), mongoTemplate), ' ');
				                    	genotypeStringCache.put(sampleGenotype.getCode(), exportedGT);
				                    }
									
									if (individualGenotypes[individualIndex] == null)
										individualGenotypes[individualIndex] = new LinkedHashSet<String>();
									individualGenotypes[individualIndex].add(exportedGT);
								}
		                	}

						for (String individual : individualPositions.keySet()) {
							int individualIndex = individualPositions.get(individual);
							if (individualGenotypeBuffers[individualIndex] == null)
								individualGenotypeBuffers[individualIndex] = new StringBuilder(initialStringBuilderCapacity.get() == 0 ? (int) (3 * markerCount) : initialStringBuilderCapacity.get());	// we are about to write individual's first genotype for this chunk

							if (individualGenotypes[individualIndex] == null)
								individualGenotypeBuffers[individualIndex].append(LINE_SEPARATOR);	// missing data
							else {
								int j = 0;
								for (String storedIndividualGenotype : individualGenotypes[individualIndex])
									individualGenotypeBuffers[individualIndex].append(storedIndividualGenotype).append((j++ == individualGenotypes[individualIndex].size() - 1 ? LINE_SEPARATOR : "|"));
							}
							if (initialStringBuilderCapacity.get() == 0)
							    initialStringBuilderCapacity.set(individualGenotypeBuffers[individualIndex].length());
						}
					});

					// write genotypes collected in this chunk to each individual's file
					for (String individual : individualPositions.keySet()) {
						int individualIndex = individualPositions.get(individual);
						BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(files[individualIndex], true));
						if (individualGenotypeBuffers[individualIndex] != null)
							os.write(individualGenotypeBuffers[individualIndex].toString().getBytes());
	
						os.close();
					}
				}
				catch (Exception e)
				{
					if (progress.getError() == null)	// only log this once
						LOG.debug("Error creating temp files", e);
					progress.setError("Error creating temp files: " + e.getMessage());
				}
			}
		};
		
		ExportManager exportManager = new ExportManager(mongoTemplate, collWithPojoCodec, VariantRunData.class, varQuery, samplesToExport, true, nQueryChunkSize, writingThread, markerCount, null, progress);
		exportManager.readAndWrite();
		
	 	if (!progress.isAborted())
	 		LOG.info("createExportFiles took " + (System.currentTimeMillis() - before)/1000d + "s to process " + markerCount + " variants and " + files.length + " individuals");
		
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