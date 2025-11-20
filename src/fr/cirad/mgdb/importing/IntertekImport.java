/** *****************************************************************************
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
 ******************************************************************************
 */
package fr.cirad.mgdb.importing;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import fr.cirad.mgdb.importing.parameters.FileImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.*;
import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import com.opencsv.CSVReader;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.Callset;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;

/**
 * The Class IntertekImport.
 */
public class IntertekImport extends AbstractGenotypeImport<FileImportParameters> {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(VariantData.class);

    /**
     * The m_process id.
     */
    //private String m_processID;

//    private boolean fImportUnknownVariants = false;

    public boolean m_fCloseContextOpenAfterImport = false;

    final static protected String validAlleleRegex = "[\\*ATGC-]+".intern();

    /**
     * Instantiates a new Intertek import.
     */
    public IntertekImport() {
    }

    /**
     * Instantiates a new Intertek import.
     *
     * @param processID the process id
     */
    public IntertekImport(String processID) {
        m_processID = processID;
    }

    /**
     * Instantiates a new Intertek import.
     */
    public IntertekImport(boolean fCloseContextOpenAfterImport) {
        this();
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    /**
     * Instantiates a new Intertek import.
     */
    public IntertekImport(String processID, boolean fCloseContextOpenAfterImport) {
        this(processID);
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            throw new Exception("You must pass 6 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, csv file, and assembly name! An optional 7th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");
        }

        File csvFile = new File(args[4]);
        if (!csvFile.exists() || csvFile.length() == 0) {
            throw new Exception("File " + args[4] + " is missing or empty!");
        }

        int mode = 0;
        try {
            mode = Integer.parseInt(args[6]);
        } catch (Exception e) {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        FileImportParameters params = new FileImportParameters(
                args[0], //sModule
                args[1], //sProject
                args[2], //sRun
                args[3], //sTechnology
                null, // nPloidy
                args[5], //assemblyName
                null, //sampleToIndividualMap
                false,//fSkipMonomorphic
                mode,//importMode
                new File(args[4]).toURI().toURL()//mainFileUrl
        );
        new IntertekImport().importToMongo(params);
    }

    @Override
    protected long doImport(FileImportParameters params, MongoTemplate mongoTemplate, GenotypingProject project, ProgressIndicator progress, Integer createdProject) throws Exception {
        String sRun = params.getRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isSkipMonomorphic();
        URL fileURL = params.getMainFileUrl();

        final String[] snpHeader = {"SNPID","SNPNum","AlleleY","AlleleX","Sequence"};
        int snpColIndex = Arrays.asList(snpHeader).indexOf("SNPID");
        int yColIndex = Arrays.asList(snpHeader).indexOf("AlleleY");
        int xColIndex = Arrays.asList(snpHeader).indexOf("AlleleX");
        String[] limit = {"Scaling"};

        List<String> dataHeaderList = Arrays.asList(new String[]{"DaughterPlate","MasterPlate","MasterWell","Call","X","Y","SNPID","SubjectID","Norm","Carrier","DaughterWell","LongID"});

        int variantColIndex = dataHeaderList.indexOf("SNPID");
        int indColIndex = dataHeaderList.indexOf("SubjectID");
        int callColIndex = dataHeaderList.indexOf("Call");
        int xFIColIndex = dataHeaderList.indexOf("X");
        int yFIColIndex = dataHeaderList.indexOf("Y");
        int masterPlateColIndex = dataHeaderList.indexOf("MasterPlate");

        readAllSampleIDsToPreloadIndividuals(fileURL, dataHeaderList, indColIndex, progress);

        Set<String> variantIdsToSave = new HashSet<>();
        HashMap<String /*variant ID*/, VariantData> variants= new HashMap<>();
        HashMap<String /*variant ID*/, List<String> /*allelesList*/> variantAllelesMap = new HashMap<>();
        m_providedIdToSampleMap = new HashMap<>();
        m_providedIdToCallsetMap = new HashMap<>();
        boolean fDbAlreadyContainedIndividuals = mongoTemplate.findOne(new Query(), Individual.class) != null;
        boolean fDbAlreadyContainedSamples = mongoTemplate.findOne(new Query(), GenotypingSample.class) != null;

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());
        Set<String> existingIds = ConcurrentHashMap.newKeySet(); //necessary to avoid ConcurrentModificationException when removing variants
        existingIds.addAll(existingVariantIDs.values());

        final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
        if (assemblyIDs.isEmpty())
            assemblyIDs.add(null);	// old-style, assembly-less DB

        int count = 0;
        Set<Individual> indsToAdd = new HashSet<>();
        Set<GenotypingSample> samplesToAdd = new HashSet<>(), samplesToUpdate = new HashSet<>();

        // Reading csv file
        // Getting alleleX and alleleY for each SNP by reading lines between lines {"SNPID","SNPNum","AlleleY","AlleleX","Sequence"} and {"Scaling"};
        // Then getting genotypes for each individual by reading lines after line {"DaughterPlate","MasterPlate","MasterWell","Call","X","Y","SNPID","SubjectID","Norm","Carrier","DaughterWell","LongID"}
        try (BufferedReader in = new BufferedReader(new InputStreamReader(fileURL.openStream())); CSVReader csvReader = new CSVReader(in)) {
            boolean snpPart = false;
            boolean dataPart = false;
            String[] values;
            int i = 0;
            int nPloidy = 0;

            String currentVariantId = null;
            HashMap<Integer, SampleGenotype> sampleGenotypes = new HashMap<>();
            HashSet<VariantRunData> variantRunsChunk = new HashSet<>();
            HashSet<VariantData> variantsChunk = new HashSet<>();

            int nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());
            int nNumberOfVariantRunsToSaveAtOnce = 0;

            while ((values = csvReader.readNext()) != null) {
                if (progress.getError() != null || progress.isAborted())
                    return 0;

                i = i+1;
                if (Arrays.asList(values).containsAll(Arrays.asList(snpHeader))) {
                    snpPart = true;
                    continue;
                }
                if (Arrays.asList(values).containsAll(Arrays.asList(limit))) {
                    snpPart = false;
                    continue;
                }

                // Reading Variants Part
                if (snpPart && !dataPart && !values[0].equals("")) {
                    String variantId = values[snpColIndex];

                    //check if variantId already exists in DB
                    VariantData variant = variantId == null ? null : mongoTemplate.findById(variantId, VariantData.class);
                    if (variant == null) {
                        variant = new VariantData(variantId);
                        String ref = values[xColIndex];
                        String alt = values[yColIndex];
                        if (!ref.matches(validAlleleRegex)) {
                            throw new Exception("Invalid ref allele '" + ref + "' provided for variant" + variantId);
                        }
                        if (!alt.matches(validAlleleRegex)) {
                            throw new Exception("Invalid ref allele '" + alt + "' provided for variant" + variantId);
                        }

                        variant.setType(Type.SNP.toString());
                        //INDEL
                        if (ref.equals("-")) {
                            ref = "N";
                            alt = "NN";
                            variant.setType(Type.INDEL.toString());
                        } else if (alt.equals("-")) {
                            ref = "NN";
                            alt = "N";
                            variant.setType(Type.INDEL.toString());
                        }
                        variant.getKnownAlleles().add(ref);
                        variant.getKnownAlleles().add(alt);
                        variantIdsToSave.add(variantId);
                    }
                    variants.put(variantId, variant);
                    variantAllelesMap.put(variantId, variant.getKnownAlleles());
                    project.getAlleleCounts().add(variant.getKnownAlleles().size());
                    continue;
                }

                // data part
                if (Arrays.asList(values).containsAll(dataHeaderList)) {
                    dataPart = true;
                    continue;
                }
                if (dataPart) {
                    String variantId = values[variantColIndex];
                    String bioEntityID = values[indColIndex];
                    String masterPlate = values[masterPlateColIndex];
                    String call = values[callColIndex];
                    String FI = values[yFIColIndex] + "," + values[xFIColIndex];

                    if (variantId.equals("") || bioEntityID.equals(""))
                        continue; //skip line if no variantId or no individualId

                    if (currentVariantId == null) {
                        currentVariantId = variantId;
                    }

                    if (!variantId.equals(currentVariantId)) {
                        if (nNumberOfVariantRunsToSaveAtOnce == 0) {
                            nNumberOfVariantRunsToSaveAtOnce = Math.max(1, nMaxChunkSize / m_providedIdToCallsetMap.size());
                        }

                        addVariantRunToChunk(currentVariantId, fSkipMonomorphic, existingIds, variantIdsToSave, variantRunsChunk, variantsChunk,
                                sampleGenotypes, variants, project, sRun, assemblyIDs);
                        sampleGenotypes = new HashMap<>();

                        if (variantRunsChunk.size() == nNumberOfVariantRunsToSaveAtOnce) {
                            //save variantRuns
                            saveChunk(variantsChunk, variantRunsChunk, existingVariantIDs, mongoTemplate, progress, saveService);
                            variantRunsChunk = new HashSet<>();
                        }
                    }
                    currentVariantId = variantId;

                    String gtCode = null;
                    List<String> variantAlleles = variantAllelesMap.get(variantId);
                    String refAllele = variantAlleles.get(0);
                    if (!call.equals("NTC")) {
                        //NTC lines are not imported (control)
                        //if genotype is ?, gtCode = null
                        if (call.contains(":")) {
                            List<String> alleles = Arrays.asList(call.split(":"));
                            gtCode = alleles.stream()
                                    .map(al -> {
                                        if (refAllele.equals("N")) {
                                            return al.equals("-") ? "0" : "1";
                                        } else if (refAllele.equals("NN")) {
                                            return al.equals("-") ? "1" : "0";
                                        } else {
                                            return al.equals(refAllele) ? "0" : "1";
                                        }
                                    })
                                    .collect(Collectors.joining("/"));
                            if (nPloidy == 0)
                                nPloidy = alleles.size();
                            else if (nPloidy != alleles.size())
                            	throw new Exception("Ploidy levels differ between variants");
                        }
                        
                    	GenotypingSample sample = null;
                    	if (sampleToIndividualMap != null) {	// provided bio-entities are actually samples
                    		if (fDbAlreadyContainedSamples) {
                    			sample = mongoTemplate.findById(bioEntityID, GenotypingSample.class);
                    			if (sample != null && !sampleToIndividualMap.isEmpty()) {	// the sample already exists in the DB, and a sample-to-individual mapping was provided for import: let's make sure individuals match
                    				String sProvidedIndividualForThisSample = determineIndividualNameAccountingForBrapiRelationships(sampleToIndividualMap, bioEntityID, progress);
                                	if (sProvidedIndividualForThisSample != null && !sample.getIndividual().equals(sProvidedIndividualForThisSample))
                                		throw new Exception("Sample " + bioEntityID + " already exists and is attached to individual " + sample.getIndividual() + ", not " + sProvidedIndividualForThisSample);
                    			}
                    		}
                    		if (sample == null) {
                                String sIndividual = determineIndividualNameAccountingForBrapiRelationships(sampleToIndividualMap, bioEntityID, progress);
                                if (sIndividual == null)
                                	throw new Exception("Unable to determine individual for sample " + bioEntityID);

                    			sample = new GenotypingSample(bioEntityID, sIndividual);
                                sample.getAdditionalInfo().put("masterPlate", masterPlate);
                                samplesToAdd.add(sample);
                    		}
                    		else
                    			samplesToUpdate.add(sample);
                    	}
                    	else {		// provided bio-entities are actually individuals
                    		sample = new GenotypingSample(bioEntityID + "-" + project.getId() + "-" + sRun, bioEntityID);
                            sample.getAdditionalInfo().put("masterPlate", masterPlate);
                            samplesToAdd.add(sample);
                    	}

                        if (!fDbAlreadyContainedIndividuals || mongoTemplate.findById(sample.getIndividual(), Individual.class) == null)  // we don't have any population data so we don't need to update the Individual if it already exists
                            indsToAdd.add(new Individual(sample.getIndividual()));

                        m_providedIdToSampleMap.put(bioEntityID, sample);  // add a sample for this individual to the project
                        if (m_providedIdToCallsetMap.get(bioEntityID) == null) {
	                        int callsetId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(Callset.class));
	                        Callset cs = new Callset(callsetId, sample, project.getId(), sRun);
	                        sample.getCallSets().add(cs);
	                        m_providedIdToCallsetMap.put(bioEntityID, cs);
                        }

                        SampleGenotype sampleGt = new SampleGenotype(gtCode);
                        sampleGt.getAdditionalInfo().put(AbstractVariantData.GT_FIELD_FI, FI);	//TODO - Check how the fluorescence indexes X et Y should be stored
                        sampleGenotypes.put(m_providedIdToCallsetMap.get(bioEntityID).getId(), sampleGt);
                    }
                }

            }
            csvReader.close();

            //Add last variantRun
            addVariantRunToChunk(currentVariantId, fSkipMonomorphic, existingIds, variantIdsToSave, variantRunsChunk,
                    variantsChunk, sampleGenotypes, variants, project, sRun, assemblyIDs);

            //save last chunk
            if (!variantRunsChunk.isEmpty())
                saveChunk(variantsChunk, variantRunsChunk, existingVariantIDs, mongoTemplate, progress, saveService);

        }

        //Insert new callsets, samples and individuals
        insertNewCallSetsSamplesIndividuals(mongoTemplate, indsToAdd, samplesToAdd, samplesToUpdate);
        setSamplesPersisted(true);

        VCFFormatHeaderLine headerLineGT = new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype");
        VCFFormatHeaderLine headerLineFI = new VCFFormatHeaderLine(AbstractVariantData.GT_FIELD_FI, 2, VCFHeaderLineType.Float, "Fluorescence intensity");
        VCFHeader header = new VCFHeader(new HashSet<>(Arrays.asList(headerLineGT, headerLineFI)));
        mongoTemplate.save(new DBVCFHeader(new DBVCFHeader.VcfHeaderId(project.getId(), sRun), header));
        return count;
    }

    private void addVariantRunToChunk(String currentVariantId, boolean fSkipMonomorphic, Set<String> existingVariantIds,
                                      Set<String> variantIdsToSave, HashSet<VariantRunData> variantRunsChunk, HashSet<VariantData> variantsChunk,
                                      HashMap<Integer, SampleGenotype> sampleGenotypes, Map<String, VariantData> variants,
                                      GenotypingProject project, String sRun, Collection<Integer> assemblyIDs) {

        if (!existingVariantIds.contains(currentVariantId) && fSkipMonomorphic) {
            String[] distinctGTs = sampleGenotypes.values().stream().map(sampleGT -> sampleGT.getCode()).filter(gtCode -> gtCode != null).distinct().toArray(String[]::new);
            if (distinctGTs.length == 0 || (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split("/")).distinct().count() < 2)) {
                variantIdsToSave.remove(currentVariantId);
                return; // skip non-variant positions that are not already known
            }
        }

        // new variant line
        VariantData variant = variants.get(currentVariantId);
        //save previous genotypes
        VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), sRun, currentVariantId));
        vrd.setKnownAlleles(variant.getKnownAlleles());
        vrd.setSampleGenotypes(sampleGenotypes);
        vrd.setType(variant.getType());
        vrd.setPositions(variant.getPositions());
        vrd.setReferencePosition(variant.getReferencePosition());
        vrd.setSynonyms(variant.getSynonyms());
        variant.getRuns().add(new Run(project.getId(), sRun));

        for (Integer asmId : assemblyIDs) {
            ReferencePosition rp = variant.getReferencePosition(asmId);
            if (rp != null)
                project.getContigs(asmId).add(rp.getSequence());
        }

        vrd.setSampleGenotypes(sampleGenotypes);

        variantRunsChunk.add(vrd);
        variantsChunk.add(variant);

    }

    @Override
    protected void initReader(FileImportParameters params) throws Exception {

    }

    @Override
    protected void closeResource() throws IOException {

    }

	private void readAllSampleIDsToPreloadIndividuals(URL fileURL, List<String> dataHeaderList, int subjectColIndex, ProgressIndicator progress) throws Exception {
		Scanner scanner = new Scanner(new File(fileURL.getFile()));
		boolean fInDataSection = false;
		HashSet<String> sampleIDs = new HashSet<>();
		while (scanner.hasNextLine()) {
			String sLine = scanner.nextLine();
			List<String> values = Helper.split(sLine, ",");
			if (values.containsAll(dataHeaderList))
				fInDataSection = true;
			else if (fInDataSection)
				sampleIDs.add(values.get(subjectColIndex));
		}
		scanner.close();
		attemptPreloadingIndividuals(sampleIDs, progress);
	}
}