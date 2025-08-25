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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import fr.cirad.mgdb.importing.parameters.FileImportParameters;
import fr.cirad.mgdb.model.mongo.maintypes.*;
import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import com.opencsv.CSVReader;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
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
        String sModule = params.getsModule();
        String sProject = params.getsRun();
        String sRun = params.getsRun();
        String assemblyName = params.getAssemblyName();
        Map<String, String> sampleToIndividualMap = params.getSampleToIndividualMap();
        boolean fSkipMonomorphic = params.isfSkipMonomorphic();
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

        Set<VariantData> variantsToSave = new HashSet<>();
        HashMap<String /*variant ID*/, List<String> /*allelesList*/> variantAllelesMap = new HashMap<>();
        HashMap<String /*variant ID*/, HashMap<Integer, SampleGenotype>> variantToSampleToGenotypeMap = new HashMap<>();
        m_providedIdToSampleMap = new HashMap<>();
        m_providedIdToCallsetMap = new HashMap<>();

        progress.addStep("Scanning existing marker IDs");
        progress.moveToNextStep();
        Assembly assembly = createAssemblyIfNeeded(mongoTemplate, assemblyName);
        HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, true, assembly == null ? null : assembly.getId());


        final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
        if (assemblyIDs.isEmpty())
            assemblyIDs.add(null);	// old-style, assembly-less DB


        // Reading csv file
        // Getting alleleX and alleleY for each SNP by reading lines between lines {"SNPID","SNPNum","AlleleY","AlleleX","Sequence"} and {"Scaling"};
        // Then getting genotypes for each individual by reading lines after line {"DaughterPlate","MasterPlate","MasterWell","Call","X","Y","SNPID","SubjectID","Norm","Carrier","DaughterWell","LongID"}
        try (BufferedReader in = new BufferedReader(new InputStreamReader(fileURL.openStream())); CSVReader csvReader = new CSVReader(in)) {
            boolean snpPart = false;
            boolean dataPart = false;
            String[] values;
            int i = 0;
            int nPloidy = 0;
            while ((values = csvReader.readNext()) != null) {
                if (progress.getError() != null || progress.isAborted())
                    return 0;

                i = i+1;
                if (Arrays.asList(values).containsAll(Arrays.asList(snpHeader))) {
                    snpPart = true;
                } else if (Arrays.asList(values).containsAll(Arrays.asList(limit))) {
                    snpPart = false;
                } else {
                    if (snpPart && !dataPart && !values[0].equals("")) {
                        String variantId = values[snpColIndex];
                        //check if variantId already exists in DB
                        VariantData variant = variantId == null ? null : mongoTemplate.findById(variantId, VariantData.class);
                        if (variant == null) {
                            variant = new VariantData(variantId);
                            variant.getKnownAlleles().add(values[yColIndex]);
                            variant.getKnownAlleles().add(values[xColIndex]);
                            variant.setType(Type.SNP.toString());
                        }
                        variantsToSave.add(variant);
                        variantAllelesMap.put(variantId, variant.getKnownAlleles());
                        project.getAlleleCounts().add(variant.getKnownAlleles().size());
                    }

                    if (Arrays.asList(values).containsAll(dataHeaderList)) {
                        dataPart = true;
                    } else {
                        if (dataPart) {
                            String variantId = values[variantColIndex];
                            String sIndOrSpId = values[indColIndex];
                            String masterPlate = values[masterPlateColIndex];
                            String call = values[callColIndex];
                            String FI = values[yFIColIndex] + "," + values[xFIColIndex];

                            if (variantId.equals("") || sIndOrSpId.equals(""))
                                continue; //skip line if no variantId or no individualId

                            if (variantToSampleToGenotypeMap.get(variantId) == null)
                                variantToSampleToGenotypeMap.put(variantId, new HashMap<>());

                            String gtCode = null;
                            List<String> variantAlleles = variantAllelesMap.get(variantId);
                            String refAllele = variantAlleles.get(0);
                            if (!call.equals("NTC")) {
                                //NTC lines are not imported (control)
                                //if genotype is ?, gtCode = null
                                if (call.contains(":")) {
                                    List<String> alleles = Arrays.asList(call.split(":"));
                                    List<String> gt = new ArrayList<>();
                                    for (String al:alleles) {
                                        if (al.equals(refAllele)) {
                                            gt.add("0");
                                        } else {
                                            gt.add("1");
                                        }
                                    }
                                    gtCode = String.join("/", gt);
                                    if (nPloidy == 0) {
                                        nPloidy = alleles.size();
                                    } else {
                                        if (nPloidy != alleles.size()) {
                                            throw new Exception("Ploidy levels differ between variants");
                                        }
                                    }
                                }

                                String sIndividual = determineIndividualName(sampleToIndividualMap, sIndOrSpId, progress);
                                if (sIndividual == null) {
                                    progress.setError("Unable to determine individual for sample " + sIndOrSpId);
                                    break;
                                }

                                GenotypingSample sample = m_providedIdToSampleMap.get(sIndOrSpId);
                                if (sample == null) {
                                    Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
                                    if (ind == null)
                                        mongoTemplate.save(new Individual(sIndividual));

                                    String sampleId = sampleToIndividualMap == null ? sIndividual : sIndOrSpId;
                                    sample = new GenotypingSample(sampleId, project.getId(), sRun, sIndividual);
                                    sample.getAdditionalInfo().put("masterPlate", masterPlate);
                                    m_providedIdToSampleMap.put(sIndOrSpId, sample);

                                    int callsetId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(CallSet.class));
                                    m_providedIdToCallsetMap.put(sIndOrSpId, new CallSet(callsetId, sampleToIndividualMap == null ? sIndividual : sIndOrSpId, sIndividual, project.getId(), params.getsRun()));
                                }

                                SampleGenotype sampleGt = new SampleGenotype(gtCode);
                                sampleGt.getAdditionalInfo().put(AbstractVariantData.GT_FIELD_FI, FI);	//TODO - Check how the fluorescence indexes X et Y should be stored

                                variantToSampleToGenotypeMap.get(variantId).put(m_providedIdToCallsetMap.get(sIndOrSpId).getId(), sampleGt);
                            }
                        }
                    }
                }
            }
            csvReader.close();

            if (variantsToSave.isEmpty()) {
                progress.setError("Found no variants to import in provided file, please check its contents!");
                return 0;
            }
        }

        mongoTemplate.insert(m_providedIdToSampleMap.values(), GenotypingSample.class);
        setSamplesPersisted(true);

        VCFFormatHeaderLine headerLineGT = new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype");
        VCFFormatHeaderLine headerLineFI = new VCFFormatHeaderLine(AbstractVariantData.GT_FIELD_FI, 2, VCFHeaderLineType.Float, "Fluorescence intensity");
        VCFHeader header = new VCFHeader(new HashSet<>(Arrays.asList(headerLineGT, headerLineFI)));
        mongoTemplate.save(new DBVCFHeader(new DBVCFHeader.VcfHeaderId(project.getId(), sRun), header));

        progress.addStep("Header was written for project " + sProject + " and run " + sRun);
        progress.moveToNextStep();
        LOG.info(progress.getProgressDescription());

        // Store variants and variantRuns
        int count = 0;
        int nNumberOfVariantsToSaveAtOnce = 1;
        int nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
        LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nNConcurrentThreads + " threads");

        /*FIXME : we should parallelize the import file parsing, similarly to what is done in other formats (although this one is not meant to contain much data...)*/
        BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
        ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());

        HashSet<VariantData> variantsChunk = new HashSet<>();
        HashSet<VariantRunData> variantRunsChunk = new HashSet<>();
        Set<String> existingIds = new HashSet<>(existingVariantIDs.values());
        for (VariantData variant : variantsToSave) {
            if (progress.getError() != null || progress.isAborted())
                break;
            if (!existingIds.contains(variant.getId()) && fSkipMonomorphic) {
                String[] distinctGTs = variantToSampleToGenotypeMap.get(variant.getVariantId()).values().stream().map(sampleGT -> sampleGT.getCode()).filter(gtCode -> gtCode != null).distinct().toArray(String[]::new);
                if (distinctGTs.length == 0 || (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split("/")).distinct().count() < 2))
                    continue; // skip non-variant positions that are not already known
            }

            VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), sRun, variant.getVariantId()));
            vrd.setKnownAlleles(variant.getKnownAlleles());
            vrd.setSampleGenotypes(variantToSampleToGenotypeMap.get(variant.getVariantId()));
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

            variantRunsChunk.add(vrd);
            variantsChunk.add(variant);

            if (count == 0) {
                nNumberOfVariantsToSaveAtOnce = Math.max(1, nMaxChunkSize / m_providedIdToSampleMap.size());
                LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);
            } else if (count % nNumberOfVariantsToSaveAtOnce == 0) {
                saveChunk(variantsChunk, variantRunsChunk, existingVariantIDs, mongoTemplate, progress, saveService);
                variantRunsChunk = new HashSet<>();
                variantsChunk = new HashSet<>();
            }
            count++;
        }

        //save last chunk
        if (!variantsChunk.isEmpty())
            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, variantsChunk, variantRunsChunk);

//        // Store the project
//        if (!project.getRuns().contains(sRun))
//            project.getRuns().add(sRun);
//        if (createdProject == null)
//            mongoTemplate.save(project);
//        else
//            mongoTemplate.insert(project);

        return count;
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