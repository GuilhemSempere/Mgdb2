package fr.cirad.mgdb.importing.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;

public abstract class SynonymAwareImport extends AbstractGenotypeImport {

    private static final Logger LOG = Logger.getLogger(SynonymAwareImport.class);
    
    protected boolean fImportUnknownVariants = false;
    
    public long importTempFileContents(ProgressIndicator progress, int nNConcurrentThreads, MongoTemplate mongoTemplate, Integer nAssemblyId, File tempFile, LinkedHashMap<String, String> providedVariantPositions, HashMap<String, String> existingVariantIDs, GenotypingProject project, String sRun, HashMap<String, ArrayList<String>> inconsistencies, LinkedHashMap<String, String> orderedIndividualToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, HashSet<Integer> indexesOfLinesThatMustBeSkipped, boolean fSkipMonomorphic) throws Exception
    {
        String[] individuals = orderedIndividualToPopulationMap.keySet().toArray(new String[orderedIndividualToPopulationMap.size()]);
        final AtomicInteger count = new AtomicInteger(0);

        // loop over each variation and write to DB
        BufferedReader reader = null;
        try
        {
            String info = "Importing genotypes";
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();
            progress.setPercentageEnabled(true);

            final int nNumberOfVariantsToSaveAtOnce = Math.max(1, nMaxChunkSize / individuals.length);
            LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);

            LinkedHashSet<String> individualsWithoutPopulation = new LinkedHashSet<>();
            for (String sIndOrSpId : orderedIndividualToPopulationMap.keySet()) {
            	GenotypingSample sample = m_providedIdToSampleMap.get(sIndOrSpId);
            	if (sample == null) {
            		progress.setError("Sample / individual mapping file contains no individual for sample " + sIndOrSpId);
            		return 0;
            	}

            	String sIndividual = sample.getIndividual();
                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
                boolean fAlreadyExists = ind != null;
                boolean fNeedToSave = true;
                if (!fAlreadyExists)
                    ind = new Individual(sIndividual);
                String sPop = orderedIndividualToPopulationMap.get(sIndOrSpId);
                if (!sPop.equals(".") && sPop.length() == 3)
                    ind.setPopulation(sPop);
                else if (!sIndividual.substring(0, 3).matches(".*\\d+.*") && sIndividual.substring(3).matches("\\d+"))
                    ind.setPopulation(sIndividual.substring(0, 3));
                else {
                    individualsWithoutPopulation.add(sIndividual);
                    if (fAlreadyExists)
                        fNeedToSave = false;
                }

                if (fNeedToSave)
                    mongoTemplate.save(ind);
            }

            if (!individualsWithoutPopulation.isEmpty())
                LOG.warn("Unable to find 3-letter population code for individuals: " + StringUtils.join(individualsWithoutPopulation, ", "));

            reader = new BufferedReader(new FileReader(tempFile));

            final BufferedReader finalReader = reader;

            // Leave one thread dedicated to the saveChunk service, it looks empirically faster that way
            int nImportThreads = Math.max(1, nNConcurrentThreads - 1);
            Thread[] importThreads = new Thread[nImportThreads];
            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());
            
            final Collection<Assembly> assemblies = mongoTemplate.findAll(Assembly.class);
            AtomicInteger nLineIndex = new AtomicInteger(-1);

            for (int threadIndex = 0; threadIndex < nImportThreads; threadIndex++) {
                importThreads[threadIndex] = new Thread() {
                    @Override
                    public void run() {
                        try {
                            long processedVariants = 0;
                            HashSet<VariantData> unsavedVariants = new HashSet<VariantData>();  // HashSet allows no duplicates
                            HashSet<VariantRunData> unsavedRuns = new HashSet<VariantRunData>();
                            while (progress.getError() == null && !progress.isAborted()) {
                                String line;
                                synchronized (finalReader) {
                                    line = finalReader.readLine();
	                                if (indexesOfLinesThatMustBeSkipped.contains(nLineIndex.incrementAndGet()))
	                                	continue;	// this is a line containing only missing data, which we don't want to persist because there is at least one non-empty line on a synonym variant, that we would lose if we did
                                }

                                if (line == null)
                                    break;

                                String[] splitLine = line.split("\t");
                                if (fSkipMonomorphic && Arrays.stream(splitLine, 1, splitLine.length).filter(gt -> !"0/0".equals(gt)).distinct().count() < 2)
                                    continue; // skip non-variant positions

                                String providedVariantId = splitLine[0];

                                String sequence = null;
                                Long bpPosition = null;
                                String pos = providedVariantPositions.get(providedVariantId);
                                String[] seqAndPos = pos == null ? null : pos.split("\t");
                                if (seqAndPos != null && seqAndPos.length == 2) 
	                                try {
	                                    bpPosition = Long.parseLong(seqAndPos[1]);
	                                	sequence = seqAndPos[0];
	                                    if ("0".equals(sequence) || 0 == bpPosition) {
	                                        sequence = null;
	                                        bpPosition = null;
	                                    }
	                                }
	                                catch (NumberFormatException nfe) {
	                                    LOG.warn("Unable to read position for variant " + providedVariantId + " - " + nfe.getMessage());
	                                }

                                String variantId = null;
                                Type type = nonSnpVariantTypeMap.get(providedVariantId);    // SNP is the default type so we don't store it in nonSnpVariantTypeMap to make it as lightweight as possible
                                for (String variantDescForPos : getIdentificationStrings(type == null ? Type.SNP.toString() : type.toString(), sequence, bpPosition, Arrays.asList(new String[] {providedVariantId}))) {
                                    variantId = existingVariantIDs.get(variantDescForPos);
                                    if (variantId != null) {
                                        if (type != null && !variantId.equals(providedVariantId))
                                            nonSnpVariantTypeMap.put(variantId, type);  // add the type to this existing variant ID so we don't miss it later on
                                        break;
                                    }
                                }

                                if (variantId == null && !fImportUnknownVariants)
                                    LOG.warn("Skipping unknown variant: " + providedVariantId);
                                else if (variantId != null && variantId.toString().startsWith("*"))
                                {
                                    LOG.warn("Skipping deprecated variant data: " + providedVariantId);
                                    continue;
                                }
                                else
                                {
                                    VariantData variant = mongoTemplate.findById(variantId == null ? providedVariantId : variantId, VariantData.class);
                                    if (variant == null)
                                        variant = new VariantData((ObjectId.isValid(providedVariantId) ? "_" : "") + providedVariantId);

                                    String[][] alleles = new String[2][individuals.length];
                                    int nIndividualIndex = 0;
                                    while (nIndividualIndex < individuals.length)
                                    {
                                        String[] genotype = (splitLine.length > nIndividualIndex + 1 ? splitLine[nIndividualIndex + 1] : "0/0").split("/");	// if some genotypes are missing at the end of the line we assume they're just missing data (may happen when discovering individual list while reorganizing genotypes from one-genotype-per-line formats where missing data is not explicitly mentioned) 
                                        if (inconsistencies != null && !inconsistencies.isEmpty()) {
                                            ArrayList<String> inconsistentIndividuals = inconsistencies.get(variant.getId());
                                            boolean fInconsistentData = inconsistencies != null && !inconsistencies.isEmpty() && inconsistentIndividuals != null && inconsistentIndividuals.contains(individuals[nIndividualIndex]);
                                            if (fInconsistentData)
                                                LOG.warn("Not adding inconsistent data: " + providedVariantId + " / " + individuals[nIndividualIndex]);

                                            alleles[0][nIndividualIndex] = fInconsistentData ? "0" : genotype[0];
                                            alleles[1][nIndividualIndex++] = fInconsistentData ? "0" : genotype[1];
                                        }
                                        else {
                                            alleles[0][nIndividualIndex] = genotype[0];
                                            alleles[1][nIndividualIndex++] = genotype[1];
                                        }
                                    }

                                    VariantRunData runToSave = addDataToVariant(mongoTemplate, variant, nAssemblyId, sequence, bpPosition, orderedIndividualToPopulationMap, nonSnpVariantTypeMap, alleles, project, sRun, fImportUnknownVariants);

                                    for (Assembly assembly : assemblies) {
                                        ReferencePosition rp = variant.getReferencePosition(assembly.getId());
                                        if (rp != null)
                                        	try {
                                        		project.getContigs(assembly.getId()).add(rp.getSequence());
                                        	}
                                        	catch (java.lang.NullPointerException npe) {
                                        		npe.printStackTrace();
                                        	}
                                    }

                                    project.getVariantTypes().add(variant.getType());					// it's a TreeSet so it will only be added if it's not already present
                                    project.getAlleleCounts().add(variant.getKnownAlleles().size());	// it's a TreeSet so it will only be added if it's not already present
                                    if (variant.getKnownAlleles().size() > 2)
                                        LOG.warn("Variant " + variant.getId() + " (" + providedVariantId + ") has more than 2 alleles!");

                                    if (variant.getKnownAlleles().size() > 0) {   // we only import data related to a variant if we know its alleles
                                        if (!unsavedVariants.contains(variant))
                                            unsavedVariants.add(variant);
                                        if (!unsavedRuns.contains(runToSave))
                                            unsavedRuns.add(runToSave);
                                    }
                                    else {
                                    	ReferencePosition rp = variant.getReferencePosition(nAssemblyId);
                                    	LOG.warn("Skipping variant " + providedVariantId + (rp != null ? " positioned at " + rp.getSequence() + ":" + rp.getStartSite() : "") + " because its alleles are not known (only missing data provided so far)");
                                    }

                                    if (processedVariants % nNumberOfVariantsToSaveAtOnce == 0) {
                                        saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, mongoTemplate, progress, saveService);
                                        unsavedVariants = new HashSet<VariantData>();
                                        unsavedRuns = new HashSet<VariantRunData>();

                                        progress.setCurrentStepProgress(count.get() * 100 / providedVariantPositions.size());
                                    }
                                }
                                int newCount = count.incrementAndGet();
                                if (newCount % (nNumberOfVariantsToSaveAtOnce*50) == 0)
                                    LOG.debug(newCount + " lines processed");
                                processedVariants += 1;
                            }

                            persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants, unsavedRuns);
                        } catch (Throwable t) {
                            progress.setError("Genotypes import failed with " + t.getClass().getSimpleName() + ": " + t.getMessage());
                            LOG.error(progress.getError(), t);
                            return;
                        }

                    }
                };

                importThreads[threadIndex].start();
            }

            for (int i = 0; i < nImportThreads; i++)
                importThreads[i].join();
            saveService.shutdown();
            saveService.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);

            if (progress.getError() != null || progress.isAborted())
            	return count.get();

            // save project data
            if (!project.getRuns().contains(sRun))
                project.getRuns().add(sRun);
            mongoTemplate.save(project);
        }
        finally
        {
            if (reader != null)
                reader.close();
        }
        return count.get();
    }

    protected VariantRunData addDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, Integer nAssemblyId, String sequence, Long bpPos, LinkedHashMap<String, String> orderedIndividualToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, String[][] alleles, GenotypingProject project, String runName, boolean fImportUnknownVariants) throws Exception
    {
        VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));

        // genotype fields
        AtomicInteger allIdx = new AtomicInteger(0);
        Map<String, Integer> alleleIndexMap = variantToFeed.getKnownAlleles().stream().collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));  // should be more efficient not to call indexOf too often...
        int i = -1;
        for (String sIndividual : orderedIndividualToPopulationMap.keySet()) {
            i++;

            if ("0".equals(alleles[0][i]) || "0".equals(alleles[1][i]))
                continue;  // Do not add missing genotypes

            for (int j = 0; j < 2; j++) { // 2 alleles per genotype
                Integer alleleIndex = alleleIndexMap.get(alleles[j][i]);
                if (alleleIndex == null && alleles[j][i].matches("[AaTtGgCc]+")) { // New allele
                    alleleIndex = variantToFeed.getKnownAlleles().size();
                    variantToFeed.getKnownAlleles().add(alleles[j][i]);
                    alleleIndexMap.put(alleles[j][i], alleleIndex);
                }
            }

            String gtCode;
            try {
                gtCode = Arrays.asList(alleles[0][i], alleles[1][i]).stream().map(allele -> alleleIndexMap.get(allele)).sorted().map(index -> index.toString()).collect(Collectors.joining("/"));
            }
            catch (Exception e) {
                LOG.warn("Ignoring invalid PLINK genotype \"" + alleles[0][i] + "/" + alleles[1][i] + "\" for variant " + variantToFeed.getId() + " and individual " + sIndividual);
                continue;
            }

            /*if (gtCode == null)
                continue;   // we don't add missing genotypes*/

            SampleGenotype aGT = new SampleGenotype(gtCode);
            vrd.getSampleGenotypes().put(m_providedIdToSampleMap.get(sIndividual).getId(), aGT);
        }
        
        if (fImportUnknownVariants && variantToFeed.getReferencePosition(nAssemblyId) == null && sequence != null) // otherwise we leave it as it is (had some trouble with overridden end-sites)
            variantToFeed.setReferencePosition(nAssemblyId, new ReferencePosition(sequence, bpPos, !variantToFeed.getKnownAlleles().isEmpty() ? bpPos + variantToFeed.getKnownAlleles().iterator().next().length() - 1 : null));

        if (!alleleIndexMap.isEmpty()) {
            Type variantType = nonSnpVariantTypeMap.get(variantToFeed.getId());
            String sVariantType = variantType == null ? Type.SNP.toString() : variantType.toString();
            if (variantToFeed.getType() == null || Type.NO_VARIATION.toString().equals(variantToFeed.getType()))
                variantToFeed.setType(sVariantType);
            else if (null != variantType && Type.NO_VARIATION != variantType && !variantToFeed.getType().equals(sVariantType))
                throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());
        }

        vrd.setKnownAlleles(variantToFeed.getKnownAlleles());
        vrd.setPositions(variantToFeed.getPositions());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
        return vrd;
    }

    /* FIXME: this mechanism could be improved to "fill holes" when genotypes are provided for some synonyms but not others (currently we import them all so the last encountered one "wins", unless it's totally empty) */
    protected HashMap<String, ArrayList<String>> checkSynonymGenotypeConsistency(File variantOrientedFile, HashMap<String, String> existingVariantIDs, Collection<String> individualsInProvidedOrder, String outputPathAndPrefix, HashSet<Integer> emptyLineIndexesToFill) throws IOException
    {
        long b4 = System.currentTimeMillis();
        LOG.info("Checking genotype consistency between synonyms...");
        String sLine = null;

        FileOutputStream inconsistencyFOS = new FileOutputStream(new File(outputPathAndPrefix + "-INCONSISTENCIES.txt"));
        HashMap<String /*existing variant id*/, ArrayList<String /*individual*/>> result = new HashMap<>();

        // first pass: identify synonym lines
        Map<String, List<Integer>> variantLinePositions = new HashMap<>();
        int nCurrentLinePos = 0;
        try (Scanner scanner = new Scanner(variantOrientedFile)) {
            while (scanner.hasNextLine()) {
                sLine = scanner.nextLine();
                String providedVariantName = sLine.substring(0, sLine.indexOf("\t"));
                String existingId = existingVariantIDs.get(providedVariantName.toUpperCase());
                if (existingId != null && !existingId.toString().startsWith("*")) {
                    List<Integer> variantLines = variantLinePositions.get(existingId);
                    if (variantLines == null) {
                        variantLines = new ArrayList<>();
                        variantLinePositions.put(existingId, variantLines);
                    };
                    variantLines.add(nCurrentLinePos);
                }
                nCurrentLinePos++;
            }
        }


        // only keep those with at least 2 synonyms
        Map<String /*variant id */, List<Integer> /*corresponding line positions*/> synonymLinePositions = variantLinePositions.entrySet().stream().filter(entry -> variantLinePositions.get(entry.getKey()).size() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        variantLinePositions.clear();   // release memory as this object is not needed anymore

        // hold all lines that will need to be compared to any other(s) in a map that makes them accessible by their line number
        HashMap<Integer, String> linesNeedingComparison = new HashMap<>();
        TreeSet<Integer> linesToReadForComparison = new TreeSet<>();
        synonymLinePositions.values().stream().forEach(varPositions -> linesToReadForComparison.addAll(varPositions));  // incrementally sorted line numbers, simplifies re-reading
        nCurrentLinePos = 0;
        try (Scanner scanner = new Scanner(variantOrientedFile)) {
            for (int nLinePos : linesToReadForComparison) {
                while (nCurrentLinePos <= nLinePos) {
                    sLine = scanner.nextLine();
                    nCurrentLinePos++;
                }
                linesNeedingComparison.put(nLinePos, sLine/*.substring(1 + sLine.indexOf("\t"))*/);
            }
        }

        // for each variant with at least two synonyms, build an array (one cell per individual) containing Sets of (distinct) encountered genotypes: inconsistencies are found where these Sets contain several items
        for (String variantId : synonymLinePositions.keySet()) {
            HashMap<String /*genotype*/, HashSet<String> /*synonyms*/>[] individualGenotypeListArray = new HashMap[individualsInProvidedOrder.size()];
            List<Integer> linesToCompareForVariant = synonymLinePositions.get(variantId);
            boolean fFoundAnyNonEmptyLineForThisVariant = false;
            for (int nLineNumber=0; nLineNumber<linesToCompareForVariant.size(); nLineNumber++) {
                String[] synAndGenotypes = linesNeedingComparison.get(linesToCompareForVariant.get(nLineNumber)).split("\t");
                boolean fFoundAnyGenotypeInThisLine = false;
                for (int individualIndex = 0; individualIndex<individualGenotypeListArray.length; individualIndex++) {
                    if (individualGenotypeListArray[individualIndex] == null)
                        individualGenotypeListArray[individualIndex] = new HashMap<>();
                    String genotype = synAndGenotypes[1 + individualIndex];
                    if (genotype.equals("0/0"))
                        continue;   // if genotype is unknown this should not keep us from considering others
                    
                    fFoundAnyGenotypeInThisLine = true;
                    HashSet<String> synonymsWithThisGenotype = individualGenotypeListArray[individualIndex].get(genotype);
                    if (synonymsWithThisGenotype == null) {
                        synonymsWithThisGenotype = new HashSet<>();
                        individualGenotypeListArray[individualIndex].put(genotype, synonymsWithThisGenotype);
                    }
                    synonymsWithThisGenotype.add(synAndGenotypes[0]);
                }                
                
                if (fFoundAnyGenotypeInThisLine)
                	fFoundAnyNonEmptyLineForThisVariant = true;
                else if (fFoundAnyNonEmptyLineForThisVariant)
                	emptyLineIndexesToFill.add(linesToCompareForVariant.get(nLineNumber)); // keep track of this empty line because if we don't skip it, saving it would wipe out genotypes we've got on other synonyms
            }

            Iterator<String> indIt = individualsInProvidedOrder.iterator();
            int individualIndex = 0;
            while (indIt.hasNext()) {
                String ind = indIt.next();
                HashMap<String, HashSet<String>> individualGenotypes = individualGenotypeListArray[individualIndex++];
                if (individualGenotypes.size() > 1) {
                    ArrayList<String> individualsWithInconsistentGTs = result.get(variantId);
                    if (individualsWithInconsistentGTs == null) {
                        individualsWithInconsistentGTs = new ArrayList<String>();
                        result.put(variantId, individualsWithInconsistentGTs);
                    }
                    individualsWithInconsistentGTs.add(ind);
                    inconsistencyFOS.write(ind.getBytes());
                    for (String gt : individualGenotypes.keySet())
                        for (String syn : individualGenotypes.get(gt))
                            inconsistencyFOS.write(("\t" + syn + "=" + gt).getBytes());
                    inconsistencyFOS.write("\r\n".getBytes());
                }
            }
        }

        inconsistencyFOS.close();
        LOG.info("Inconsistency file was saved to " + outputPathAndPrefix + "-INCONSISTENCIES.txt" + " in " + (System.currentTimeMillis() - b4) / 1000 + "s");
        return result;
    }
    
    protected void createSamples(MongoTemplate mongoTemplate, int projId, String sRun, HashMap<String, String> sampleToIndividualMap, LinkedHashMap<String, String> orderedIndividualToPopulationMap, ProgressIndicator progress) {
        m_providedIdToSampleMap = new HashMap<String /*individual*/, GenotypingSample>();
        HashSet<Individual> indsToAdd = new HashSet<>();
        boolean fDbAlreadyContainedIndividuals = mongoTemplate.findOne(new Query(), Individual.class) != null;
        for (String sIndOrSpId : orderedIndividualToPopulationMap.keySet()) {
        	String sIndividual = sampleToIndividualMap == null ? sIndOrSpId : sampleToIndividualMap.get(sIndOrSpId);
        	if (sIndividual == null) {
        		progress.setError("Sample / individual mapping file contains no individual for sample " + sIndOrSpId);
        		return;
        	}
        	
            if (!fDbAlreadyContainedIndividuals || mongoTemplate.findById(sIndividual, Individual.class) == null)  // we don't have any population data so we don't need to update the Individual if it already exists
                indsToAdd.add(new Individual(sIndividual));

            if (!indsToAdd.isEmpty() && indsToAdd.size() % 1000 == 0) {
            	mongoTemplate.insert(indsToAdd, Individual.class);
                indsToAdd = new HashSet<>();
            }

            int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
            m_providedIdToSampleMap.put(sIndOrSpId, new GenotypingSample(sampleId, projId, sRun, sIndividual, sampleToIndividualMap == null ? null : sIndOrSpId));   // add a sample for this individual to the project
        }
        
        mongoTemplate.insert(m_providedIdToSampleMap.values(), GenotypingSample.class);
        if (!indsToAdd.isEmpty()) {
        	mongoTemplate.insert(indsToAdd, Individual.class);
            indsToAdd = null;
        }	                    					
        m_fSamplesPersisted = true;
    }
}