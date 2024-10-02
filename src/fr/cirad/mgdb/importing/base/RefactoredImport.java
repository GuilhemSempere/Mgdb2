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
import java.util.LinkedList;
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
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.model.mongo.maintypes.Assembly;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.Run;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongo.subtypes.VariantRunDataId;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.AutoIncrementCounter;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * Base class for genotyping data import procedures that need refactoring provided files in a marker-oriented manner
 * 
 * @author sempere
 */
public abstract class RefactoredImport extends AbstractGenotypeImport {

    private static final Logger LOG = Logger.getLogger(RefactoredImport.class);
    
    protected boolean m_fImportUnknownVariants = false;
    protected int m_ploidy = 2;
    protected Integer m_maxExpectedAlleleCount = null;	// if set, will issue warnings when exceeded
    final static protected String validAlleleRegex = "[\\*AaTtGgCcNnIiDd]+".intern();
    
	public void setMaxExpectedAlleleCount(Integer maxExpectedAlleleCount) {
		m_maxExpectedAlleleCount = maxExpectedAlleleCount;
	}
    
	public void setPloidy(int ploidy) {
		m_ploidy = ploidy;
	}
	
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
            		progress.setError("Sample / individual mapping contains no individual for sample " + sIndOrSpId);
            		return 0;
            	}

            	String sIndividual = sample.getIndividual();
                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
                boolean fAlreadyExists = ind != null;
                boolean fNeedToSave = true;
                if (!fAlreadyExists)
                    ind = new Individual(sIndividual);
                String sPop = orderedIndividualToPopulationMap.get(sIndOrSpId);
                if (sPop != null)
                    ind.setPopulation(sPop);
                else {
                    String firstEncounteredDigit = sIndividual.chars().filter(Character::isDigit).mapToObj(Character::toString).findFirst().orElse(null);
                    Integer firstDigitPos = firstEncounteredDigit == null ? null : sIndividual.indexOf(firstEncounteredDigit);
                    if (firstDigitPos != null && sIndividual.length() > firstDigitPos && !sIndividual.substring(0, firstDigitPos).matches(".*\\d+.*") && sIndividual.substring(firstDigitPos).matches("\\d+"))
	                    ind.setPopulation(sIndividual.substring(0, firstDigitPos));
	                else {
	                    individualsWithoutPopulation.add(sIndividual);
	                    if (fAlreadyExists)
	                        fNeedToSave = false;
	                }
                }

                if (fNeedToSave)
                    mongoTemplate.save(ind);
            }

            if (!individualsWithoutPopulation.isEmpty() && populationCodesExpected())
                LOG.warn("Unable to find population code for individuals: " + StringUtils.join(individualsWithoutPopulation, ", "));

            reader = new BufferedReader(new FileReader(tempFile));

            final BufferedReader finalReader = reader;

            // Leave one thread dedicated to the saveChunk service, it looks empirically faster that way
            int nImportThreads = Math.max(1, nNConcurrentThreads - 1);
            Thread[] importThreads = new Thread[nImportThreads];
            BlockingQueue<Runnable> saveServiceQueue = new LinkedBlockingQueue<Runnable>(saveServiceQueueLength(nNConcurrentThreads));
            ExecutorService saveService = new ThreadPoolExecutor(1, saveServiceThreads(nNConcurrentThreads), 30, TimeUnit.SECONDS, saveServiceQueue, new ThreadPoolExecutor.CallerRunsPolicy());
            
            final Collection<Integer> assemblyIDs = mongoTemplate.findDistinct(new Query(), "_id", Assembly.class, Integer.class);
            if (assemblyIDs.isEmpty())
            	assemblyIDs.add(null);	// old-style, assembly-less DB
            AtomicInteger nLineIndex = new AtomicInteger(-1), ignoredVariants = new AtomicInteger();
            
            final int projId = project.getId();

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
	                                if (indexesOfLinesThatMustBeSkipped != null && indexesOfLinesThatMustBeSkipped.contains(nLineIndex.incrementAndGet()))
	                                	continue;	// this is a line containing only missing data, which we don't want to persist because there is at least one non-empty line on a synonym variant, that we would lose if we did
                                }

                                if (line == null)
                                    break;

                                String[] splitLine = line.split("\t");
                                String providedVariantId = splitLine[0];

                                String sequence = null;
                                Long bpPosition = null;
                                String pos = providedVariantPositions.get(providedVariantId);
                                String[] seqAndPos = pos == null ? null : pos.split("\t");
                                if (seqAndPos != null && seqAndPos.length == 2) 
	                                try {
	                                    bpPosition = Long.parseLong(seqAndPos[1].replace(",", ""));
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
                                
                                if (variantId == null && fSkipMonomorphic) {
                                	String[] distinctGTs = Arrays.stream(splitLine, 1, splitLine.length).filter(gt -> !gt.isEmpty()).distinct().toArray(String[]::new);
                                	if (distinctGTs.length == 0 || (distinctGTs.length == 1 && Arrays.stream(distinctGTs[0].split("/")).distinct().count() < 2))
										continue; // skip non-variant positions that are not already known
                                }

                                if (variantId == null && !m_fImportUnknownVariants) {
                                    LOG.warn("Skipping unknown variant: " + providedVariantId);
                                    ignoredVariants.incrementAndGet();
                                }
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

                                    variant.getRuns().add(new Run(projId, sRun));
                                    
                                    String[][] alleles = new String[individuals.length][m_ploidy];
                                    int nIndividualIndex = 0;
                                    while (nIndividualIndex < individuals.length) {
	                                    if (splitLine.length > nIndividualIndex + 1 && !"".equals(splitLine[nIndividualIndex + 1])) {	// if some genotypes are missing at the end of the line we assume they're just missing data (may happen when discovering individual list while reorganizing genotypes from one-genotype-per-line formats where missing data is not explicitly mentioned)
	                                        String[] genotype = splitLine[nIndividualIndex + 1].split("/");
	                                        boolean fInconsistentData = false;
	                                        if (inconsistencies != null && !inconsistencies.isEmpty()) {
	                                            ArrayList<String> inconsistentIndividuals = inconsistencies.get(variant.getId());
	                                            fInconsistentData = inconsistencies != null && !inconsistencies.isEmpty() && inconsistentIndividuals != null && inconsistentIndividuals.contains(individuals[nIndividualIndex]);
	                                        }

                                            if (fInconsistentData)
                                                LOG.warn("Not adding inconsistent data: " + providedVariantId + " / " + individuals[nIndividualIndex]);
                                            else 
	                                            alleles[nIndividualIndex] = genotype;
	                                    }
                                        nIndividualIndex++;
                                    }

                                    VariantRunData runToSave = addDataToVariant(mongoTemplate, variant, nAssemblyId, sequence, bpPosition, orderedIndividualToPopulationMap, nonSnpVariantTypeMap, alleles, project, sRun, m_fImportUnknownVariants);
                                    if (m_maxExpectedAlleleCount != null && variant.getKnownAlleles().size() > m_maxExpectedAlleleCount)
                                        LOG.warn("Variant " + variant.getId() + " (" + providedVariantId + ") has more than " + m_maxExpectedAlleleCount + " alleles!");

                                    if (variant.getKnownAlleles().size() > 0) {   // we only import data related to a variant if we know its alleles
                                        if (!unsavedVariants.contains(variant))
                                            unsavedVariants.add(variant);
                                        if (!unsavedRuns.contains(runToSave))
                                            unsavedRuns.add(runToSave);
                                        
                                        for (Integer asmId : assemblyIDs) {
                                            ReferencePosition rp = variant.getReferencePosition(asmId);
                                            if (rp != null)
                                            	project.getContigs(asmId).add(rp.getSequence());
                                        }
                                        
                                        project.getVariantTypes().add(variant.getType());					// it's a TreeSet so it will only be added if it's not already present
                                        project.getAlleleCounts().add(variant.getKnownAlleles().size());	// it's a TreeSet so it will only be added if it's not already present
                                    }
                                    else {
                                    	ReferencePosition rp = variant.getReferencePosition(nAssemblyId);
                                    	LOG.info("Skipping variant " + providedVariantId + (rp != null ? " positioned at " + rp.getSequence() + ":" + rp.getStartSite() : "") + " because its alleles are not known (only missing data provided so far)");
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
            
            if (ignoredVariants.get() > 0)
            	LOG.warn("Number of ignored variants: " + ignoredVariants);

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

    protected boolean populationCodesExpected() {
		return false;
	}

	protected VariantRunData addDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, Integer nAssemblyId, String sequence, Long bpPos, LinkedHashMap<String, String> orderedIndOrSpToPopulationMap, Map<String, Type> nonSnpVariantTypeMap, String[][] alleles, GenotypingProject project, String runName, boolean fImportUnknownVariants) throws Exception
    {
        VariantRunData vrd = new VariantRunData(new VariantRunDataId(project.getId(), runName, variantToFeed.getId()));

        // genotype fields
        AtomicInteger allIdx = new AtomicInteger(0);
        Map<String, Integer> alleleIndexMap = variantToFeed.getKnownAlleles().stream().collect(Collectors.toMap(Function.identity(), t -> allIdx.getAndIncrement()));  // should be more efficient not to call indexOf too often...
        int i = -1;
        for (String sIndOrSp : orderedIndOrSpToPopulationMap.keySet()) {
            i++;

            if (alleles[i][0] == null)
                continue;  // Do not add missing genotypes

            for (int j=0; j<alleles[i].length; j++) {
				if (!alleles[i][j].matches(validAlleleRegex))
                	throw new Exception("Invalid allele '" + alleles[i][j] + "' provided for " + sIndOrSp + " at variant" + variantToFeed.getId());

            	if ("I".equals(alleles[i][j]))
            		alleles[i][j] = "NN";
            	else if ("D".equals(alleles[i][j]))
            		alleles[i][j] = "N";

                Integer alleleIndex = alleleIndexMap.get(alleles[i][j]);
                if (alleleIndex != null)
                	continue;	// we already have this one

                alleleIndex = variantToFeed.getKnownAlleles().size();
                variantToFeed.getKnownAlleles().add(alleles[i][j]);
                alleleIndexMap.put(alleles[i][j], alleleIndex);
            }
            
            try {
            	Stream<String> alleleStream;
            	if (alleles[i].length == 1 && m_ploidy > 1) {	// it's a collapsed homozygous that we need to expand
            		LinkedList<String> alleleList = new LinkedList<>();
            		while (alleleList.size() < m_ploidy)
            			alleleList.add(alleles[i][0]);
            		alleleStream = alleleList.stream();
            	}
            	else
            		alleleStream = Arrays.stream(alleles[i]);

            	SampleGenotype aGT = new SampleGenotype(alleleStream.map(allele -> alleleIndexMap.get(allele)).sorted().map(index -> index.toString()).collect(Collectors.joining("/")));
                vrd.getSampleGenotypes().put(m_providedIdToSampleMap.get(sIndOrSp).getId(), aGT);
            }
            catch (Exception e) {
                LOG.warn("Ignoring invalid genotype \"" + String.join("/", alleles[i]) + "\" for variant " + variantToFeed.getId() + " and individual " + sIndOrSp, e);
            }
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
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
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
                linesNeedingComparison.put(nLinePos, sLine);
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
                	if (synAndGenotypes.length <= 1 + individualIndex)
                		break;	// otherwise there's only missing data left for this synonym

                    String genotype = synAndGenotypes[1 + individualIndex];
                    if (genotype.isEmpty())
                        continue;   // if genotype is unknown this should not keep us from considering others

                    if (individualGenotypeListArray[individualIndex] == null)
                        individualGenotypeListArray[individualIndex] = new HashMap<>();

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
                if (individualGenotypes != null && individualGenotypes.size() > 1) {
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
    
    protected void createSamples(MongoTemplate mongoTemplate, int projId, String sRun, HashMap<String, String> sampleToIndividualMap, LinkedHashMap<String, String> orderedIndividualToPopulationMap, ProgressIndicator progress) throws Exception {
        m_providedIdToSampleMap = new HashMap<String /*individual*/, GenotypingSample>();
        HashSet<Individual> indsToAdd = new HashSet<>();
        boolean fDbAlreadyContainedIndividuals = mongoTemplate.findOne(new Query(), Individual.class) != null;
        attemptPreloadingIndividuals(orderedIndividualToPopulationMap.keySet(), progress);
        for (String sIndOrSpId : orderedIndividualToPopulationMap.keySet()) {
        	String sIndividual = determineIndividualName(sampleToIndividualMap, sIndOrSpId, progress);
        	if (sIndividual == null) {
        		progress.setError("Unable to determine individual for sample " + sIndOrSpId);
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
        
        // make sure provided sample names do not conflict with existing ones
        if (mongoTemplate.findOne(new Query(Criteria.where(GenotypingSample.FIELDNAME_NAME).in(m_providedIdToSampleMap.values().stream().map(sp -> sp.getSampleName()).toList())), GenotypingSample.class) != null) {
	        progress.setError("Some of the sample IDs provided in the mapping file already exist in this database!");
	        return;
		}

        mongoTemplate.insert(m_providedIdToSampleMap.values(), GenotypingSample.class);
        if (!indsToAdd.isEmpty()) {
        	mongoTemplate.insert(indsToAdd, Individual.class);
            indsToAdd = null;
        }	                    					
        setSamplesPersisted(true);
    }
}