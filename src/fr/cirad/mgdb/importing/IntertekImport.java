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

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.opencsv.CSVReader;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The Class IntertekImport.
 */
public class IntertekImport extends AbstractGenotypeImport {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(VariantData.class);

    /**
     * The m_process id.
     */
    private String m_processID;

    private boolean fImportUnknownVariants = false;

    public boolean m_fCloseContextOpenAfterImport = false;

    /**
     * Instantiates a new PLINK import.
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
        if (args.length < 5) {
            throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, csv file! An optional 6th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");
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
        new IntertekImport().importToMongo(args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), false, mode);
    }

    /**
     * Import to mongo.
     *
     * @param sModule the module
     * @param sProject the project
     * @param sRun the run
     * @param sTechnology the technology
     * @param fileURL
     * @param importMode the import mode
     * @param fSkipMonomorphic whether or not to skip import of variants that have no polymorphism (where all individuals have the same genotype)
     * @return a project ID if it was created by this method, otherwise null
     * @throws Exception the exception
     */
    public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, URL fileURL, boolean fSkipMonomorphic, int importMode) throws Exception {
        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
        progress.setPercentageEnabled(false);        
        
        // not compatible java 1.8 ? 
        // FeatureReader<VariantContext> reader = AbstractFeatureReader.getFeatureReader(mainFilePath, fIsBCF ? new BCF2Codec() : new VCFCodec(), false);
        GenericXmlApplicationContext ctx = null;
        try {
            MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null) {	// we are probably being invoked offline
                try {
                    ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
                } catch (BeanDefinitionStoreException fnfe) {
                    LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
                    ctx = new GenericXmlApplicationContext("applicationContext.xml");
                }

                MongoTemplateManager.initialize(ctx);
                mongoTemplate = MongoTemplateManager.get(sModule);
                if (mongoTemplate == null) {
                    throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
                }
            }

            if (m_processID == null) {
                m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();
            }

            final String[] snpHeader = {"SNPID","SNPNum","AlleleY","AlleleX","Sequence"};
            int snpColIndex = Arrays.asList(snpHeader).indexOf("SNPID");
            int yColIndex = Arrays.asList(snpHeader).indexOf("AlleleY");
            int xColIndex = Arrays.asList(snpHeader).indexOf("AlleleX");
            String[] limit = {"Scaling"};

            final String[] dataHeader = {"DaughterPlate","MasterPlate","MasterWell","Call","X","Y","SNPID","SubjectID","Norm","Carrier","DaughterWell","LongID"};
            int variantColIndex = Arrays.asList(dataHeader).indexOf("SNPID");
            int indColIndex = Arrays.asList(dataHeader).indexOf("SubjectID");
            int callColIndex = Arrays.asList(dataHeader).indexOf("Call");
            int xFIColIndex = Arrays.asList(dataHeader).indexOf("X");
            int yFIColIndex = Arrays.asList(dataHeader).indexOf("Y");
            int masterPlateColIndex = Arrays.asList(dataHeader).indexOf("MasterPlate");

            Set<VariantData> variantsToSave = new HashSet<>();
            HashMap<String /*variant ID*/, List<String> /*allelesList*/> variantAllelesMap = new HashMap<>();
            HashMap<String /*variant ID*/, HashMap<Integer, SampleGenotype>> variantSamplesMap = new HashMap<>();
            HashMap<String /*individual ID*/, GenotypingSample> samples = new HashMap<>();

            GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            MongoTemplateManager.lockProjectForWriting(sModule, sProject);
            cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

            Integer createdProject = null;
            // create project if necessary
            if (project == null || importMode == 2) {	// create it
                project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
                project.setName(sProject);
                project.setOrigin(2 /* Sequencing */);
                project.setTechnology(sTechnology);
                project.getVariantTypes().add(Type.SNP.toString());
                createdProject = project.getId();
            }
            
            VCFFormatHeaderLine headerLineGT = new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype");
            VCFFormatHeaderLine headerLineFI = new VCFFormatHeaderLine("FI", 2, VCFHeaderLineType.Float, "Fluorescence intensity");
            VCFHeader header = new VCFHeader(new HashSet<>(Arrays.asList(headerLineGT, headerLineFI)));
            mongoTemplate.save(new DBVCFHeader(new DBVCFHeader.VcfHeaderId(project.getId(), sRun), header));
            
            progress.addStep("Header was written for project " + sProject + " and run " + sRun);
            progress.moveToNextStep();
            LOG.info(progress.getProgressDescription());

            HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false);
            
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

                        if (Arrays.asList(values).containsAll(Arrays.asList(dataHeader))) {
                            dataPart = true;
                        } else {
                            if (dataPart) {
                                String variantId = values[variantColIndex];
                                String individualId = values[indColIndex];
                                String masterPlate = values[masterPlateColIndex];
                                String call = values[callColIndex];
                                String FI = values[yFIColIndex] + "," + values[xFIColIndex];
                                
                                if (variantId.equals("") || individualId.equals("")) {
                                    continue; //skip line if no variantId or no individualId
                                }

                                if (variantSamplesMap.get(variantId) == null) {
                                    variantSamplesMap.put(variantId, new HashMap<>());
                                }

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

                                    if (samples.get(individualId) == null) {
                                        Individual ind = mongoTemplate.findById(individualId, Individual.class);
                                        if (ind == null) {
                                            ind = new Individual(individualId);
                                            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
                                            map.put("masterPlate", masterPlate);
                                            ind.setAdditionalInfo(map);
                                            mongoTemplate.save(ind);
                                        }

                                        int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                                        samples.put(individualId, new GenotypingSample(sampleId, project.getId(), sRun, individualId));
                                    }

                                    SampleGenotype sampleGt = new SampleGenotype(gtCode);
                                    HashMap aiMap = new HashMap<>();

                                    //TODO - Check how the fluorescence indexes X et Y should be stored
                                    aiMap.put("FI", FI);
                                    sampleGt.setAdditionalInfo(aiMap);
                                    variantSamplesMap.get(variantId).put(samples.get(individualId).getId(), sampleGt);                                             
                                }
                            }
                        }
                    }
                }
                project.setPloidyLevel(nPloidy);
                csvReader.close();
            }

            // Store variants and variantRuns
            int count = 0;
            int nNumberOfVariantsToSaveAtOnce = 1;
            final ArrayList<Thread> threadsToWaitFor = new ArrayList<>();
            int chunkIndex = 0;
            int nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
            LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nNConcurrentThreads + " threads");
            
            /*FIXME : we should parallelize the import file parsing, similarly to what is done in other formats (although this one is not meant to contain much data...)*/
            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());

            HashSet<VariantData> variantsChunk = new HashSet<>();
            HashSet<VariantRunData> variantRunsChunk = new HashSet<>();
            Set<String> existingIds = new HashSet<>(existingVariantIDs.values());
            for (VariantData variant : variantsToSave) {                
                if (!existingIds.contains(variant.getId()) && fSkipMonomorphic && variantSamplesMap.get(variant.getVariantId()).values().stream().map(sampleGT -> sampleGT.getCode()).filter(gtCode -> gtCode != null).distinct().count() < 2)
                    continue;

                VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), sRun, variant.getVariantId()));
                vrd.setKnownAlleles(variant.getKnownAlleles());
                vrd.setSampleGenotypes(variantSamplesMap.get(variant.getVariantId()));
                vrd.setType(variant.getType());
                vrd.setReferencePosition(variant.getReferencePosition());                
                vrd.setSynonyms(variant.getSynonyms());
                variantRunsChunk.add(vrd);
                variantsChunk.add(variant);

                if (count == 0) {
                    nNumberOfVariantsToSaveAtOnce = Math.max(1, nMaxChunkSize / samples.size());
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
            
            // Store the project
            // always save project before samples otherwise the sample cleaning procedure in MgdbDao.prepareDatabaseForSearches may remove them if called in the meantime
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            if (createdProject == null)
                mongoTemplate.save(project);
            else
                mongoTemplate.insert(project);

            // Store samples
            mongoTemplate.insert(samples.values(), GenotypingSample.class);
            
            progress.addStep("Preparing database for searches");
            progress.moveToNextStep();
            MgdbDao.prepareDatabaseForSearches(mongoTemplate);

            LOG.info("IntertekImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");		
            progress.markAsComplete();
            return createdProject;
        } finally {
            if (m_fCloseContextOpenAfterImport && ctx != null)
                ctx.close();
            MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
        }
    }
}
