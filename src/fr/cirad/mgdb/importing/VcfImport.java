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

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.Sequence;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.bcf2.BCF2Codec;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFContigHeaderLine;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

/**
 * The Class VcfImport.
 */
public class VcfImport extends AbstractGenotypeImport {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(VariantData.class);

    public static final String ANNOTATION_FIELDNAME_EFF = "EFF";
    public static final String ANNOTATION_FIELDNAME_ANN = "ANN";
    public static final String ANNOTATION_FIELDNAME_CSQ = "CSQ";

    public boolean m_fCloseContextOpenAfterImport = false;

    /**
     * The m_process id.
     */
    private String m_processID;

    /**
     * Instantiates a new vcf import.
     */
    public VcfImport() {
        this("random_process_" + System.currentTimeMillis() + "_" + Math.random());
    }

    /**
     * Instantiates a new vcf import.
     */
    public VcfImport(boolean fCloseContextOpenAfterImport) {
        this();
        m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    /**
     * Instantiates a new vcf import.
     *
     * @param processID the process id
     */
    public VcfImport(String processID) {
        m_processID = processID;
    }

    /**
     * Instantiates a new vcf import.
     */
    public VcfImport(String processID, boolean fCloseContextOpenAfterImport) {
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
            throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, and VCF file! Two optionals parameters: 6th supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list)");
        }

        File mainFile = new File(args[4]);
        if (!mainFile.exists() || mainFile.length() == 0) {
            throw new Exception("File " + args[4] + " is missing or empty!");
        }

        int mode = 0;
        try {
            mode = Integer.parseInt(args[5]);
        } catch (Exception e) {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        new VcfImport().importToMongo(args[4].toLowerCase().endsWith(".bcf"), args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), false, mode);
    }

    /**
     * Import to mongo.
     *
     * @param fIsBCF whether or not it is a bcf
     * @param sModule the module
     * @param sProject the project
     * @param sRun the run
     * @param sTechnology the technology
     * @param mainFileUrl the main file URL
     * @param fSkipMonomorphic whether or not to skip import of variants that have no polymorphism (where all individuals have the same genotype)
     * @param importMode the import mode
     * @return a project ID if it was created by this method, otherwise null
     * @throws Exception the exception
     */
    public Integer importToMongo(boolean fIsBCF, String sModule, String sProject, String sRun, String sTechnology, URL mainFileUrl, boolean fSkipMonomorphic, int importMode) throws Exception {
        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"}); // better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
        progress.setPercentageEnabled(false);

        FeatureReader<VariantContext> reader;

        if (fIsBCF) {
            BCF2Codec bc = new BCF2Codec();
            reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), bc, false);
        } else {
            VCFCodec vc = new VCFCodec();
            reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), vc, false);
        }
        // not compatible java 1.8 ?
        // FeatureReader<VariantContext> reader = AbstractFeatureReader.getFeatureReader(mainFilePath, fIsBCF ? new BCF2Codec() : new VCFCodec(), false);
        GenericXmlApplicationContext ctx = null;
        try {
            MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null) {    // we are probably being invoked offline
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

            GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);

            Iterator<VariantContext> variantIterator = reader.iterator();
            int nPloidy = 0, i = 0;
            while (variantIterator.hasNext() && i++ < 100 && nPloidy == 0)
            {
                VariantContext vcfEntry = variantIterator.next();
                if (vcfEntry.getCommonInfo().getAttribute("CNV") == null)
                {
                    nPloidy = vcfEntry.getMaxPloidy(0);
                    LOG.info("Found ploidy level of " + nPloidy + " for " + vcfEntry.getType() + " variant " + vcfEntry.getContig() + ":" + vcfEntry.getStart());
                    break;
                }
            }
            if (importMode == 0 && project != null && project.getPloidyLevel() != nPloidy)
                throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + nPloidy + ") data!");

            MongoTemplateManager.lockProjectForWriting(sModule, sProject);

            cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

            VCFHeader header = (VCFHeader) reader.getHeader();
            int effectAnnotationPos = -1, geneIdAnnotationPos = -1;
            for (VCFInfoHeaderLine headerLine : header.getInfoHeaderLines()) {
                if (ANNOTATION_FIELDNAME_EFF.equals(headerLine.getID()) || ANNOTATION_FIELDNAME_ANN.equals(headerLine.getID()) || ANNOTATION_FIELDNAME_CSQ.equals(headerLine.getID())) {
                    String desc = headerLine.getDescription().replaceAll("\\(", "").replaceAll("\\)", "");
                    desc = desc.substring(1 + desc.indexOf(":")).replace("'", "");
                    String[] fields = desc.split("\\|");
                    for (i = 0; i<fields.length; i++) {
                        String trimmedField = fields[i].trim();
                        if (/*EFF*/ "Gene_Name".equals(trimmedField) || /*EFF*/ "Gene_ID".equals(trimmedField) || /*CSQ or ANN*/ "Gene".equals(trimmedField)) {
                            geneIdAnnotationPos = i;
                        } else if (/*EFF*/ "Annotation".equals(trimmedField) || /*CSQ or ANN*/ "Consequence".equals(trimmedField)) {
                            effectAnnotationPos = i;
                        }
                    }
                }
            }

            for (VCFContigHeaderLine contigLine : header.getContigLines()) {
                Map<String, String> lineFields = contigLine.getGenericFields();
                String sAssembly = lineFields.get("assembly"), sLength = lineFields.get("length");
                if (sAssembly != null || sLength != null) {
                    Sequence seq = new Sequence(contigLine.getID());
                    seq.setAssembly(sAssembly);
                    if (sLength != null)
                        seq.setLength(Long.parseLong(sLength));
                    mongoTemplate.save(seq);
                }
            }

            Integer createdProject = null;
            // create project if necessary
            if (project == null || importMode == 2) {   // create it
                project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
                project.setName(sProject);
                project.setOrigin(2 /* Sequencing */);
                project.setTechnology(sTechnology);
                createdProject = project.getId();
            }
            project.setPloidyLevel(nPloidy);

            mongoTemplate.save(new DBVCFHeader(new VcfHeaderId(project.getId(), sRun), header));

            progress.addStep("Header was written for project " + sProject + " and run " + sRun);
            progress.moveToNextStep();
            LOG.info(progress.getProgressDescription());

            HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false);

            int nNumberOfVariantsToSaveAtOnce = -1;
            variantIterator = reader.iterator();
            progress.addStep("Processing variant lines");
            progress.moveToNextStep();

            AtomicInteger totalProcessedVariantCount = new AtomicInteger(0);
            String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());

            int nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());

            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());
            List<VariantContextHologram> vcChunk = new ArrayList<>();
            HashMap<String /*individual*/, Comparable> phasingGroups = new HashMap<String /*individual*/, Comparable>();
            final ConcurrentLinkedDeque<Thread> importThreads = new ConcurrentLinkedDeque<>();
            final MongoTemplate finalMongoTemplate = mongoTemplate;
            final GenotypingProject finalProject = project;
            final int finalEffectAnnotationPos = effectAnnotationPos, finalGeneIdAnnotationPos = geneIdAnnotationPos;

            HashMap<String /*individual*/, GenotypingSample> individualToSampleMap = new HashMap<String /*individual*/, GenotypingSample>();
            List<Individual> indsToAdd = new ArrayList<>();
            boolean fDbAlreadyContainedIndividuals = mongoTemplate.findOne(new Query(), Individual.class) != null, fDbAlreadyContainedVariants = mongoTemplate.findOne(new Query() {{ fields().include("_id"); }}, VariantData.class) != null;
            for (String sIndividual : header.getSampleNamesInOrder()) {
                if (!fDbAlreadyContainedIndividuals || mongoTemplate.findById(sIndividual, Individual.class) == null)  // we don't have any population data so we don't need to update the Individual if it already exists
                    indsToAdd.add(new Individual(sIndividual));

                if (!indsToAdd.isEmpty() && indsToAdd.size() % 1000 == 0) {
                    mongoTemplate.insert(indsToAdd, Individual.class);
                    indsToAdd = new ArrayList<>();
                }

                int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                individualToSampleMap.put(sIndividual, new GenotypingSample(sampleId, project.getId(), sRun, sIndividual));   // add a sample for this individual to the project
            }
            if (!indsToAdd.isEmpty()) {
                mongoTemplate.insert(indsToAdd, Individual.class);
                indsToAdd = null;
            }

            // loop over each variation
            while (variantIterator.hasNext()) {
                if (progress.getError() != null || progress.isAborted())
                    return null;

                VariantContext vcfEntry = variantIterator.next();
                if (vcfEntry.getCommonInfo().hasAttribute(""))
                    vcfEntry.getCommonInfo().removeAttribute("");   // working around cases where the info field accidentally ends with a semicolon
                vcChunk.add(new VariantContextHologram(vcfEntry));

                if (nNumberOfVariantsToSaveAtOnce == -1) {
                    nNumberOfVariantsToSaveAtOnce = (int) (vcfEntry.getSampleNames().isEmpty() ? nMaxChunkSize : Math.max(1, Math.ceil((float) nMaxChunkSize / (/*nNConcurrentThreads * */vcfEntry.getSampleNames().size()))));
                    LOG.info("Importing project '" + sProject + "' into " + sModule + " by chunks of size " + nNumberOfVariantsToSaveAtOnce + " using " + nNConcurrentThreads + " threads");
                }

                if (vcChunk.size() == nNumberOfVariantsToSaveAtOnce || !variantIterator.hasNext()) {
                    final List<VariantContextHologram> vcChunkToImport = vcChunk;
                    Thread t = new Thread() {
                        public void run() {
                            try
                            {
                                List<VariantData> unsavedVariants = new ArrayList<>();
                                List<VariantRunData> unsavedRuns = new ArrayList<>();
                                for (VariantContextHologram vcfEntry : vcChunkToImport) {
                                    String variantId = null;
                                    for (String variantDescForPos : getIdentificationStrings(vcfEntry.getType().toString(), vcfEntry.getContig(), (long) vcfEntry.getStart(), Arrays.asList(new String[] {vcfEntry.getID()})))
                                    {
                                        variantId = existingVariantIDs.get(variantDescForPos);
                                        if (variantId != null)
                                            break;
                                    }

                                   if (variantId == null && fSkipMonomorphic && !vcfEntry.isVariant())
                                        continue; // skip non-variant positions that are not already known

                                    VariantData variant = variantId == null || !fDbAlreadyContainedVariants ? null : finalMongoTemplate.findById(variantId, VariantData.class);
                                    if (variant == null) {
                                        if (vcfEntry.hasID()) {
                                            variant = new VariantData((ObjectId.isValid(vcfEntry.getID()) ? "_" : "") + vcfEntry.getID());
                                            totalProcessedVariantCount.getAndIncrement();
                                        }
                                        else
                                            variant = new VariantData(generatedIdBaseString + String.format(String.format("%09x", totalProcessedVariantCount.getAndIncrement())));
                                    }
                                    else
                                        totalProcessedVariantCount.getAndIncrement();

                                    unsavedVariants.add(variant);
                                    VariantRunData runToSave = addVcfDataToVariant(finalMongoTemplate, header, variant, vcfEntry, finalProject, sRun, phasingGroups, individualToSampleMap, finalEffectAnnotationPos, finalGeneIdAnnotationPos);
                                    if (!unsavedRuns.contains(runToSave))
                                        unsavedRuns.add(runToSave);

                                    finalProject.getAlleleCounts().add(variant.getKnownAlleles().size());    // it's a Set so it will only be added if it's not already present
                                    finalProject.getVariantTypes().add(vcfEntry.getType().toString());   // it's a Set so it will only be added if it's not already present
                                    finalProject.getSequences().add(vcfEntry.getContig());  // it's a Set so it will only be added if it's not already present
                                }

                                saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, finalMongoTemplate, progress, saveService);
                                progress.setCurrentStepProgress(totalProcessedVariantCount.get());
                                if (!importThreads.contains(this) && progress.getCurrentStepProgress() % (vcChunkToImport.size()*50) == 0)
                                    LOG.debug(progress.getCurrentStepProgress() + " lines processed");
                            }
                            catch (Exception e)
                            {
                                progress.setError("Error occured importing variant number " + (totalProcessedVariantCount.get() + 1) + " (" + vcfEntry.getType().toString() + ":" + vcfEntry.getContig() + ":" + vcfEntry.getStart() + ")");
                                LOG.error("Error", e);
                            }
                            finally {
                                importThreads.remove(this);
                            }
                        }
                    };

                    if (importThreads.size() < nNConcurrentThreads - 1) {
                        importThreads.add(t);
                        t.start();
                    }
                    else
                        t.run();

                    vcChunk = new ArrayList<>();
                }
            }
            reader.close();

            for (Thread t : importThreads)
                t.join();

            saveService.shutdown();
            saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

            if (progress.getError() != null || progress.isAborted())
                return null;

            // always save project before samples otherwise the sample cleaning procedure in MgdbDao.prepareDatabaseForSearches may remove them if called in the meantime
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            if (createdProject == null)
                mongoTemplate.save(project);
            else
                mongoTemplate.insert(project);
            mongoTemplate.insert(individualToSampleMap.values(), GenotypingSample.class);

            progress.addStep("Preparing database for searches");
            progress.moveToNextStep();
            MgdbDao.prepareDatabaseForSearches(mongoTemplate);

            LOG.info("VcfImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + totalProcessedVariantCount + " records");
            progress.markAsComplete();
            return createdProject;
        }
        finally
        {
            if (m_fCloseContextOpenAfterImport && ctx != null)
                ctx.close();

            reader.close();
            MongoTemplateManager.unlockProjectForWriting(sModule, sProject);
        }
    }

    /**
     * Adds the vcf data to variant.
     *
     * @param mongoTemplate the mongo template
     * @param header the VCF Header
     * @param variantToFeed the variant to feed
     * @param vc the VariantContext
     * @param project the project
     * @param runName the run name
     * @param phasingGroup the phasing group
     * @param individualToSampleMap the used samples
     * @param effectAnnotationPos the effect annotation pos
     * @param geneIdAnnotationPos the gene name annotation pos
     * @return the variant run data
     * @throws Exception the exception
     */
    static private VariantRunData addVcfDataToVariant(MongoTemplate mongoTemplate, VCFHeader header, VariantData variantToFeed, VariantContextHologram vc, GenotypingProject project, String runName, HashMap<String /*individual*/, Comparable> phasingGroup, Map<String /*individual*/, GenotypingSample> individualToSampleMap, int effectAnnotationPos, int geneIdAnnotationPos) throws Exception
    {
        // mandatory fields
        if (variantToFeed.getType() == null) {
            variantToFeed.setType(vc.getType().toString());
        } else if (!variantToFeed.getType().equals(vc.getType().toString())) {
            throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());
        }

        List<String> knownAlleleList = new ArrayList<String>();
        if (variantToFeed.getKnownAlleles().size() > 0)
            knownAlleleList.addAll(variantToFeed.getKnownAlleles());
        ArrayList<String> allelesInVC = new ArrayList<String>();
        allelesInVC.add(vc.getReference().getBaseString());
        for (Allele alt : vc.getAlternateAlleles())
            allelesInVC.add(alt.getBaseString());
        for (String vcAllele : allelesInVC)
            if (!knownAlleleList.contains(vcAllele))
                knownAlleleList.add(vcAllele);
        variantToFeed.setKnownAlleles(knownAlleleList);

        if (variantToFeed.getReferencePosition() == null) // otherwise we leave it as it is (had some trouble with overridden end-sites)
            variantToFeed.setReferencePosition(new ReferencePosition(vc.getContig(), vc.getStart(), (long) vc.getEnd()));

        VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));

        // main VCF fields that are stored as additional info in the DB
        if (vc.isFullyDecoded())
            vrd.getAdditionalInfo().put(VariantData.FIELD_FULLYDECODED, true);
        if (vc.hasLog10PError())
            vrd.getAdditionalInfo().put(VariantData.FIELD_PHREDSCALEDQUAL, vc.getPhredScaledQual());
        if (!VariantData.FIELDVAL_SOURCE_MISSING.equals(vc.getSource()))
            vrd.getAdditionalInfo().put(VariantData.FIELD_SOURCE, vc.getSource());
        if (vc.filtersWereApplied())
            vrd.getAdditionalInfo().put(VariantData.FIELD_FILTERS, vc.getFilters().size() > 0 ? Helper.arrayToCsv(",", vc.getFilters()) : VCFConstants.PASSES_FILTERS_v4);

        List<String> aiEffect = new ArrayList<String>(), aiGene = new ArrayList<String>();

        // actual VCF info fields
        Map<String, Object> attributes = vc.getAttributes();
        for (String key : attributes.keySet()) {
            if (geneIdAnnotationPos != -1 && (ANNOTATION_FIELDNAME_EFF.equals(key) || ANNOTATION_FIELDNAME_ANN.equals(key) || ANNOTATION_FIELDNAME_CSQ.equals(key))) {
                Object effectAttr = vc.getAttributes().get(key);
                List<String> effectList = effectAttr instanceof String ? Arrays.asList((String) effectAttr) : (List<String>) effectAttr;
                for (String effect : effectList) {
                    for (String effectDesc : effect.split(",")) {
                        String sEffect = null;
                        int parenthesisPos = !ANNOTATION_FIELDNAME_EFF.equals(key) ? -1 /* parenthesis can also be used in ANN or CSQ, but differently */ : effectDesc.indexOf("(");
                        List<String> fields = Helper.split(effectDesc.substring(parenthesisPos + 1).replaceAll("\\)", ""), "|");
                        if (parenthesisPos > 0)
                            sEffect = effectDesc.substring(0, parenthesisPos);  // snpEff version < 4.1
                        else if (effectAnnotationPos != -1)
                            sEffect = fields.get(effectAnnotationPos);
                        if (sEffect != null)
                            aiEffect.add(sEffect);
                        aiGene.add(fields.get(geneIdAnnotationPos));
                    }
                }
                vrd.getAdditionalInfo().put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, aiGene);
                vrd.getAdditionalInfo().put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, aiEffect);
                for (String variantEffectAnnotation : aiEffect) {
                    if (variantEffectAnnotation != null && !project.getEffectAnnotations().contains(variantEffectAnnotation)) {
                        project.getEffectAnnotations().add(variantEffectAnnotation);
                    }
                }
            }

            Object attrVal = vc.getAttributes().get(key);
            if (attrVal instanceof ArrayList) {
                vrd.getAdditionalInfo().put(key, Helper.arrayToCsv(",", (ArrayList) attrVal));
            } else if (attrVal != null) {
                if (attrVal instanceof Boolean && ((Boolean) attrVal).booleanValue()) {
                    vrd.getAdditionalInfo().put(key, (Boolean) attrVal);
                } else {
                    try {
                        int intVal = Integer.valueOf(attrVal.toString());
                        vrd.getAdditionalInfo().put(key, intVal);
                    } catch (NumberFormatException nfe1) {
                        try {
                            double doubleVal = Double.valueOf(attrVal.toString());
                            vrd.getAdditionalInfo().put(key, doubleVal);
                        } catch (NumberFormatException nfe2) {
                            vrd.getAdditionalInfo().put(key, attrVal.toString());
                        }
                    }
                }
            }
        }

        // genotype fields
        Iterator<Genotype> genotypes = vc.getGenotypesOrderedByName().iterator();
        Map<String, Integer> knownAlleleStringToIndexMap = new HashMap<>();
        for (int i=0; i<knownAlleleList.size(); i++)
            knownAlleleStringToIndexMap.put(knownAlleleList.get(i), i);

        while (genotypes.hasNext()) {
            Genotype genotype = genotypes.next();

            boolean isPhased = genotype.isPhased();
            String sIndividual = genotype.getSampleName();

            Comparable phasedGroup = phasingGroup.get(sIndividual);
            if (phasedGroup == null || (!isPhased && !genotype.isNoCall()))
                phasingGroup.put(sIndividual, variantToFeed.getId());

            List<String> gtAllelesAsStrings = genotype.getAlleles().stream().map(allele -> allele.getBaseString()).collect(Collectors.toList());

            String gtCode = VariantData.rebuildVcfFormatGenotype(knownAlleleStringToIndexMap, gtAllelesAsStrings, isPhased, false);
            if ("1/0".equals(gtCode))
                gtCode = "0/1"; // convert to "0/1" so that MAF queries can work reliably

            SampleGenotype aGT = new SampleGenotype(gtCode);
            if (isPhased) {
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_GT, VariantData.rebuildVcfFormatGenotype(knownAlleleStringToIndexMap, gtAllelesAsStrings, isPhased, true));
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_ID, phasingGroup.get(sIndividual));
            }
            if (genotype.hasGQ()) {
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_GQ, genotype.getGQ());
            }
            if (genotype.hasDP()) {
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_DP, genotype.getDP());
            }
            boolean fSkipPlFix = false; // for performance
            if (genotype.hasAD()) {
                int[] adArray = genotype.getAD(), originalAdArray = adArray;
                adArray = VariantData.fixAdFieldValue(adArray, vc.getAlleles(), knownAlleleList);
                if (originalAdArray == adArray)
                    fSkipPlFix = true;  // if AD was correct then PL is too
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_AD, Helper.arrayToCsv(",", adArray));
            }
            if (genotype.hasPL()) {
                int[] plArray = genotype.getPL();
                if (!fSkipPlFix)
                    plArray = VariantData.fixPlFieldValue(plArray, genotype.getPloidy(), vc.getAlleles(), knownAlleleList);
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PL, Helper.arrayToCsv(",", plArray));
            }
            Map<String, Object> extendedAttributes = genotype.getExtendedAttributes();
            for (String sAttrName : extendedAttributes.keySet()) {
                VCFFormatHeaderLine formatHeaderLine = header.getFormatHeaderLine(sAttrName);
                if (formatHeaderLine != null) {
                    boolean fConvertToNumber = (formatHeaderLine.getType().equals(VCFHeaderLineType.Integer) || formatHeaderLine.getType().equals(VCFHeaderLineType.Float)) && formatHeaderLine.isFixedCount() && formatHeaderLine.getCount() == 1;
                    String value = extendedAttributes.get(sAttrName).toString();
                    Object correctlyTypedValue = fConvertToNumber ? Float.parseFloat(value) : value;
                    if (fConvertToNumber && !formatHeaderLine.getType().equals(VCFHeaderLineType.Float))
                        correctlyTypedValue = Math.round((float) correctlyTypedValue);
                    aGT.getAdditionalInfo().put(sAttrName, correctlyTypedValue);
                }
            }

            if (genotype.isFiltered())
                aGT.getAdditionalInfo().put(VariantData.FIELD_FILTERS, genotype.getFilters());

            if (genotype.isCalled() || !aGT.getAdditionalInfo().isEmpty())  // otherwise there's no point in persisting an empty object
                vrd.getSampleGenotypes().put(individualToSampleMap.get(sIndividual).getId(), aGT);
        }

        vrd.setKnownAlleles(variantToFeed.getKnownAlleles());
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
        return vrd;
    }

    /**
     *
     * @author sempere
     * This is some kind of DAO for VariantContext, needed because the latter is not thread-safe
     *
     */
    static public class VariantContextHologram {
        private Type type;
        private List<Allele>  alleles;
        private Iterable<Genotype> genotypesOrderedByName;
        private Map<String, Object> attributes;
        private boolean filtersWereApplied;
        private Set<String> filters;
        private double phredScaledQual;
        private String source;
        private boolean hasLog10PError;
        private boolean isFullyDecoded;
        private long start;
        private long end;
        private String contig;
        private List<Allele> alternateAlleles;
        private Allele reference;
        private boolean hasID;
        private String id;
        private boolean isVariant;

        public VariantContextHologram(VariantContext vc) {
            hasID = vc.hasID();
            id = vc.getID();
            isVariant = vc.isVariant();
            type = vc.getType();
            alleles = vc.getAlleles();
            genotypesOrderedByName = vc.getGenotypesOrderedByName();
            attributes = vc.getAttributes();
            filtersWereApplied = vc.filtersWereApplied();
            filters = vc.getFilters();
            phredScaledQual = vc.getPhredScaledQual();
            source = vc.getSource();
            hasLog10PError = vc.hasLog10PError();
            isFullyDecoded = vc.isFullyDecoded();
            start = vc.getStart();
            end = vc.getEnd();
            contig = vc.getContig();
            alternateAlleles = vc.getAlternateAlleles();
            reference = vc.getReference();
        }

        public boolean hasID() {
            return hasID;
        }

        public boolean isVariant() {
            return isVariant;
        }

        public String getID() {
            return id;
        }

        public Type getType() {
            return type;
        }

        public List<Allele>  getAlleles() {
            return alleles;
        }

        public Iterable<Genotype> getGenotypesOrderedByName() {
            return genotypesOrderedByName;
        }

        public Map<String, Object> getAttributes() {
            return attributes;
        }

        public boolean filtersWereApplied() {
            return filtersWereApplied;
        }

        public Set<String> getFilters() {
            return filters;
        }

        public double getPhredScaledQual() {
            return phredScaledQual;
        }

        public String getSource() {
            return source;
        }

        public boolean hasLog10PError() {
            return hasLog10PError;
        }

        public boolean isFullyDecoded() {
            return isFullyDecoded;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public String getContig() {
            return contig;
        }

        public List<Allele> getAlternateAlleles() {
            return alternateAlleles;
        }

        public Allele getReference() {
            return reference;
        }

    }

//    public static void printGenotypes(int j, int k, String genotype)
//    {
//       if (genotype.length()==k)
//       {
//           LOG.info("genotype " + genotype + " has length " + k);
//       }
//       else
//       {
//           for (int a=0; a<j; ++a)
//           {
//               String s = "" + (char)(a+65);
//               s += genotype;
//               printGenotypes(a+1, k, s);
//           }
//       }
//    }
}