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
package fr.cirad.mgdb.model.mongo.subtypes;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.bson.codecs.pojo.annotations.BsonProperty;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.tools.Helper;
import fr.cirad.tools.SetUniqueListWithConstructor;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContext.Type;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFConstants;

abstract public class AbstractVariantData
{    
    /** The Constant FIELDNAME_ANALYSIS_METHODS. */
    public final static String FIELDNAME_ANALYSIS_METHODS = "am";
    
    /** The Constant FIELDNAME_SYNONYMS. */
    public final static String FIELDNAME_SYNONYMS = "sy";
    
    /** The Constant FIELDNAME_KNOWN_ALLELES. */
    public final static String FIELDNAME_KNOWN_ALLELES = "ka";
    
    /** The Constant FIELDNAME_TYPE. */
    public final static String FIELDNAME_TYPE = "ty";
    
    /** The Constant FIELDNAME_REFERENCE_POSITION. */
    public final static String FIELDNAME_REFERENCE_POSITION = "rp";
    
    /** The Constant FIELDNAME_PROJECT_DATA. */
    public final static String FIELDNAME_PROJECT_DATA = "pj";
    
    /** The Constant FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA. */
    public final static String FIELDNAME_SYNONYM_TYPE_ID_ILLUMINA = "il";
    
    /** The Constant FIELDNAME_SYNONYM_TYPE_ID_NCBI. */
    public final static String FIELDNAME_SYNONYM_TYPE_ID_NCBI = "nc";
    
    /** The Constant FIELDNAME_SYNONYM_TYPE_ID_INTERNAL. */
    public final static String FIELDNAME_SYNONYM_TYPE_ID_INTERNAL = "in";
    
    /** The Constant SECTION_ADDITIONAL_INFO. */
    public final static String SECTION_ADDITIONAL_INFO = "ai";

    /** The Constant FIELD_PHREDSCALEDQUAL. */
    public static final String FIELD_PHREDSCALEDQUAL = "qual";
    
    /** The Constant FIELD_SOURCE. */
    public static final String FIELD_SOURCE = "name";
    
    /** The Constant FIELD_FILTERS. */
    public static final String FIELD_FILTERS = "filt";
    
    /** The Constant FIELD_FULLYDECODED. */
    public static final String FIELD_FULLYDECODED = "fullDecod";
    
    /** The Constant FIELDVAL_SOURCE_MISSING. */
    public static final String FIELDVAL_SOURCE_MISSING = "Unknown";
    
    /** The Constant GT_FIELD_GQ. */
    public static final String GT_FIELD_GQ = "GQ";
    
    /** The Constant GT_FIELD_DP. */
    public static final String GT_FIELD_DP = "DP";
    
    /** The Constant GT_FIELD_AD. */
    public static final String GT_FIELD_AD = "AD";
    
    /** The Constant GT_FIELD_PL. */
    public static final String GT_FIELD_PL = "PL";
    
    /** The Constant GT_FIELD_PHASED_GT. */
    public static final String GT_FIELD_PHASED_GT = "phGT";
    
    /** The Constant GT_FIELD_PHASED_ID. */
    public static final String GT_FIELD_PHASED_ID = "phID";

    /** The Constant GT_FIELDVAL_AL_MISSING. */
    public static final String GT_FIELDVAL_AL_MISSING = ".";
    
    /** The Constant GT_FIELDVAL_ID_MISSING. */
    public static final String GT_FIELDVAL_ID_MISSING = ".";

    /** The type. */
    @BsonProperty(FIELDNAME_TYPE)
    @Field(FIELDNAME_TYPE)
    private String type;

    /** The reference position. */
    @BsonProperty(FIELDNAME_REFERENCE_POSITION)
    @Field(FIELDNAME_REFERENCE_POSITION)
    ReferencePosition referencePosition = null;
    
    /** The synonyms. */
    @BsonProperty(FIELDNAME_SYNONYMS)
    @Field(FIELDNAME_SYNONYMS)
    private TreeMap<String /*synonym type*/, TreeSet<String>> synonyms;

    /** The analysis methods. */
    @BsonProperty(FIELDNAME_ANALYSIS_METHODS)
    @Field(FIELDNAME_ANALYSIS_METHODS)
    private TreeSet<String> analysisMethods = null;

    /** The known alleles. */
    @BsonProperty(FIELDNAME_KNOWN_ALLELES)
    @Field(FIELDNAME_KNOWN_ALLELES)
    protected SetUniqueListWithConstructor<String> knownAlleles;

    /** The additional info. */
    @BsonProperty(SECTION_ADDITIONAL_INFO)
    @Field(SECTION_ADDITIONAL_INFO)
    private HashMap<String, Object> additionalInfo = null;

    static private HashSet<String> specificallyTreatedAdditionalInfoFields = new HashSet<String> () {{ add(FIELD_SOURCE); add(FIELD_FULLYDECODED); add(FIELD_FILTERS); add(FIELD_PHREDSCALEDQUAL); }};
      
    /**
     * Fixes AD array in the case where provided alleles are different from the order in which we have them in the DB
     * @param importedAD
     * @param importedAlleles
     * @param knownAlleles
     * @return
     */
    static public int[] fixAdFieldValue(int[] importedAD, List<? extends Comparable> importedAlleles, List<String> knownAlleles)
    {
        List<String> importedAllelesAsStrings = importedAlleles.stream().filter(allele -> Allele.class.isAssignableFrom(allele.getClass()))
                    .map(Allele.class::cast)
                    .map(allele -> allele.getBaseString()).collect(Collectors.toList());
        
        if (importedAllelesAsStrings.isEmpty())
            importedAllelesAsStrings.addAll((Collection<? extends String>) importedAlleles);
        
        if (Arrays.equals(knownAlleles.toArray(), importedAllelesAsStrings.toArray()))
        {
//            System.out.println("AD: no fix needed for " + Helper.arrayToCsv(", ", importedAD));
            return importedAD;
        }

        HashMap<Integer, Integer> knownAlleleToImportedAlleleIndexMap = new HashMap<>();
        for (int i=0; i<importedAlleles.size(); i++)
        {
            String allele = importedAllelesAsStrings.get(i);
            int knownAlleleIndex = knownAlleles.indexOf(allele);
            if (knownAlleleToImportedAlleleIndexMap.get(knownAlleleIndex) == null)
                knownAlleleToImportedAlleleIndexMap.put(knownAlleleIndex, i);
        }
        int[] adToStore = new int[knownAlleles.size()];
        for (int i=0; i<adToStore.length; i++)
        {
            Integer importedAlleleIndex = knownAlleleToImportedAlleleIndexMap.get(i);
            adToStore[i] = importedAlleleIndex == null ? 0 : importedAD[importedAlleleIndex];
        }
//        System.out.println("AD: " + Helper.arrayToCsv(", ", importedAD) + " -> " + Helper.arrayToCsv(", ", adToStore));

        return adToStore;
    }

    static public int[] fixPlFieldValue(int[] importedPL, int ploidy, List<? extends Comparable> importedAlleles, List<String> knownAlleles)
    {
        List<String> importedAllelesAsStrings = importedAlleles.stream().filter(allele -> Allele.class.isAssignableFrom(allele.getClass()))
                .map(Allele.class::cast)
                .map(allele -> allele.getBaseString()).collect(Collectors.toList());
        
        if (importedAllelesAsStrings.isEmpty())
            importedAllelesAsStrings.addAll((Collection<? extends String>) importedAlleles);
    
        if (Arrays.equals(knownAlleles.toArray(), importedAllelesAsStrings.toArray()))
        {
//            System.out.println("PL: no fix needed for " + Helper.arrayToCsv(", ", importedPL));
            return importedPL;
        }
        
        HashMap<Integer, Integer> knownAlleleToImportedAlleleIndexMap = new HashMap<>();
        for (int i=0; i<importedAlleles.size(); i++)
        {
            String allele = importedAllelesAsStrings.get(i);
            int knownAlleleIndex = knownAlleles.indexOf(allele);
            if (knownAlleleToImportedAlleleIndexMap.get(knownAlleleIndex) == null)
                knownAlleleToImportedAlleleIndexMap.put(knownAlleleIndex, i);
        }
        
        int[] plToStore = new int[bcf_ap2g(knownAlleles.size(), ploidy)];
        for (int i=0; i<plToStore.length; i++)
        {
            int[] genotype = bcf_ip2g(i, ploidy);
            for (int j=0; j<genotype.length; j++)    // convert genotype to match the provided allele ordering
            {
                Integer importedAllele = knownAlleleToImportedAlleleIndexMap.get(genotype[j]);
                if (importedAllele == null)
                {
                    genotype = null;
                    break;    // if any allele is not part of the imported ones then the whole genotype is not represented
                }
                else
                    genotype[j] = importedAllele;
            }
            if (genotype != null)
                Arrays.sort(genotype);
            
            plToStore[i] = genotype == null ? Integer.MAX_VALUE : importedPL[(int) bcf_g2i(genotype, ploidy)];
        }
//        System.out.println("PL: " + Helper.arrayToCsv(", ", importedPL) + " -> " + Helper.arrayToCsv(", ", plToStore));
        
        return plToStore;
    }
    
    /**
     * Gets number of genotypes from number of alleles and ploidy.
     * Translated from original C++ code that was part of the project https://github.com/atks/vt
     */
    static public int bcf_ap2g(int no_allele, int no_ploidy)
    {
        if (no_ploidy==1 || no_allele<=1)
            return no_allele;
        else if (no_ploidy==2)
            return (((no_allele+1)*(no_allele))>>1);
        else
            return (int) Helper.choose(no_ploidy+no_allele-1, no_allele-1);
    }
        
    /**
     * Gets index of a genotype of n ploidy.
     * Translated from original C++ code that was part of the project https://github.com/atks/vt
     */
    static public int bcf_g2i(int[] g, int n)
    {
        if (n==1)
            return g[0];
        if (n==2)
            return g[0] + (((g[1]+1)*(g[1]))>>1);
        else
        {
            int index = 0;
            for (int i=0; i<n; ++i)
                index += bcf_ap2g(g[i], i+1);
            return index;
        }
    }
    
    /**
     * Gets genotype from genotype index and ploidy.
     * Translated from original C++ code that was part of the project https://github.com/atks/vt
     */
    static public int[] bcf_ip2g(int genotype_index, int no_ploidy)
    {
        int[] genotype = new int[no_ploidy];
        int pth = no_ploidy;
        int max_allele_index = genotype_index;
        int leftover_genotype_index = genotype_index;
        while (pth>0)
        {
            for (int allele_index=0; allele_index <= max_allele_index; ++allele_index)
            {
                double i = Helper.choose(pth+allele_index-1, pth);
                if (i>=leftover_genotype_index || allele_index==max_allele_index)
                {
                    if (i>leftover_genotype_index)
                        --allele_index;
                    leftover_genotype_index -= Helper.choose(pth+allele_index-1, pth);
                    --pth;
                    max_allele_index = allele_index;
                    genotype[pth] = allele_index;
                    break;                
                }
            }
        }
        return genotype;
    }
  
//  static public List<Integer> getAlleles(final int PLindex, final int ploidy) {
//      if ( ploidy == 2 ) { // diploid
//          final GenotypeLikelihoodsAllelePair pair = getAllelePair(PLindex);
//          return Arrays.asList(pair.alleleIndex1, pair.alleleIndex2);
//      } else { // non-diploid
//          if (!anyploidPloidyToPLIndexToAlleleIndices.containsKey(ploidy))
//              throw new IllegalStateException("Must initialize the cache of allele anyploid indices for ploidy " + ploidy);
//
//          if (PLindex < 0 || PLindex >= anyploidPloidyToPLIndexToAlleleIndices.get(ploidy).size()) {
//              final String msg = "The PL index " + PLindex + " does not exist for " + ploidy + " ploidy, " +
//                      (PLindex < 0 ? "cannot have a negative value." : "initialized the cache of allele anyploid indices with the incorrect number of alternate alleles.");
//              throw new IllegalStateException(msg);
//          }
//
//          return anyploidPloidyToPLIndexToAlleleIndices.get(ploidy).get(PLindex);
//      }
//}
    
//    static public int likelihoodGtIndex(int j, int k)
//    {
//        return (k*(k+1)/2)+j;
//    }
//    
//    /**
//     * Instantiates a new variant data.
//     */
//    public VariantData() {
//        super();
//    }
//    
//    /**
//     * Instantiates a new variant data.
//     *
//     * @param id the id
//     */
//    public VariantData(Comparable id) {
//        super();
//        this.id = id;
//    }
//    
//    /**
//     * Gets the id.
//     *
//     * @return the id
//     */
//    public Comparable getId() {
//        return id;
//    }

    /**
     * Gets the synonyms.
     *
     * @return the synonyms
     */
    public TreeMap<String, TreeSet<String>> getSynonyms() {
        return synonyms;
    }

    /**
     * Sets the synonyms.
     *
     * @param synonyms the synonyms
     */
    public void setSynonyms(TreeMap<String, TreeSet<String>> synonyms) {
        this.synonyms = synonyms;
    }
    
    public TreeSet<String> getSynonyms(String type) {
        if (synonyms == null)
            return null;
        return synonyms.get(type);
    }

    public void putSynonyms(String type, TreeSet<String> synSet) {
        if (synonyms == null)
            synonyms = new TreeMap<>();
        synonyms.put(type, synSet);
    }

    public TreeSet<String> getAnalysisMethods() {
        return analysisMethods;
    }

    public void setAnalysisMethods(TreeSet<String> analysisMethods) {
        this.analysisMethods = analysisMethods;
    }

    /**
     * Gets the type.
     *
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type.
     *
     * @param type the new type
     */
    public void setType(String type) {
        this.type = type == null ? null : type.intern();
    }

    /**
     * Gets the reference position.
     *
     * @return the reference position
     */
    public ReferencePosition getReferencePosition() {
        return referencePosition;
    }

    /**
     * Sets the reference position.
     *
     * @param referencePosition the new reference position
     */
    public void setReferencePosition(ReferencePosition referencePosition) {
        this.referencePosition = referencePosition;
    }
    
    /**
     * Gets the known alleles.
     *
     * @return the known alleles
     */
    public SetUniqueListWithConstructor<String> getKnownAlleles() {
        if (knownAlleles == null)
            knownAlleles = new SetUniqueListWithConstructor<String>();
        return knownAlleles;
    }

    /**
     * Sets the known allele list.
     *
     * @param knownAlleles the new known allele list
     */
    public void setKnownAlleles(List<String> knownAlleles) {
        this.knownAlleles = new SetUniqueListWithConstructor(knownAlleles);
        for (String allele : this.knownAlleles)
            allele.intern();
    }
    
    /**
     * Gets the additional info.
     *
     * @return the additional info
     */
    public HashMap<String, Object> getAdditionalInfo() {
        if (additionalInfo == null)
            additionalInfo = new HashMap<>();
        return additionalInfo;
    }

    /**
     * Sets the additional info.
     *
     * @param additionalInfo the additional info
     */
    public void setAdditionalInfo(HashMap<String, Object> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }
    
    /**
     * Static get alleles from genotype code.
     *
     * @param alleleList the allele list
     * @param code the code
     * @return the list
     * @throws Exception the exception
     */
    static public List<String> staticGetAllelesFromGenotypeCode(List<String> alleleList, String code) throws NoSuchElementException
    {
        if (code == null)
            return new ArrayList<>(0);

        try    {
            return Arrays.stream(code.split("[\\|/]")).map(alleleCodeIndex -> alleleList.get(Integer.parseInt(alleleCodeIndex))).collect(Collectors.toList());
        }
        catch (IndexOutOfBoundsException ioobe) {
            throw new NoSuchElementException("Variant has no such allele: " + ioobe.getMessage());
        }
    }

    /**
     * Safely gets the alleles from genotype code (retrieves eventual missing alleles from corresponding VariantData document)
     *
     * @param code the code
     * @param mongoTemplate the MongoTemplate to use for fixing allele list if incomplete
     * @return the alleles from genotype code
     * @throws Exception the exception
     */
    public List<String> safelyGetAllelesFromGenotypeCode(String code, MongoTemplate mongoTemplate) throws NoSuchElementException
    {
        try {
            return staticGetAllelesFromGenotypeCode(getKnownAlleles(), code);
        }
        catch (NoSuchElementException e1) {
            setKnownAlleles(mongoTemplate.findById(getVariantId(), VariantData.class).getKnownAlleles());
            mongoTemplate.save(this);
            try {
                return staticGetAllelesFromGenotypeCode(getKnownAlleles(), code);
            }
            catch (NoSuchElementException e2) {
                throw new NoSuchElementException("Variant " + this + " - " + e2.getMessage());
            }
        }
    }

    /**
     * Gets the alleles from genotype code.
     *
     * @param code the code
     * @return the alleles from genotype code
     * @throws Exception the exception
     */
    public List<String> getAllelesFromGenotypeCode(String code) throws NoSuchElementException
    {
        try
        {
            return staticGetAllelesFromGenotypeCode(getKnownAlleles(), code);
        }
        catch (NoSuchElementException e)
        {
            throw new NoSuchElementException("Variant " + this + " - " + e.getMessage());
        }
    }
    
    /**
     * Rebuild vcf format genotype.
     *
     * @param knownAlleleStringToIndexMap map providing the index corresponding to each allele
     * @param genotypeAlleles the genotype alleles
     * @param fPhased whether or not the genotype is phased
     * @param keepCurrentPhasingInfo the keep current phasing info
     * @return the string
     * @throws Exception the exception
     */
    public static String rebuildVcfFormatGenotype(Map<String, Integer> knownAlleleStringToIndexMap, List<String> genotypeAlleles, boolean fPhased, boolean keepCurrentPhasingInfo) throws Exception
    {
    	if (genotypeAlleles.contains("."))
    		return null;

        StringBuilder result = new StringBuilder();
        String separator = keepCurrentPhasingInfo && fPhased ? "|" : "/";
        for (String gtA : genotypeAlleles) {
            Integer alleleIndex = knownAlleleStringToIndexMap.get(gtA);
            if (alleleIndex == null && !GT_FIELDVAL_AL_MISSING.equals(gtA))
                throw new Exception("Unable to find allele '" + gtA + "' in alternate list");
            
            if (result.length() != 0)
                result.append(separator);
            result.append(alleleIndex);
        }

        return result.length() > 0 ? result.toString() : null;
    }
        
    /**
     * To variant context.
     *
     * @param MongoTemplate the mongoTemplate
     * @param runs the runs
     * @param exportVariantIDs the export variant ids
     * @param samplesToExport overall list of samples involved in the export
     * @param individualPositions map providing the index at which each individual must appear in the export file
     * @param individuals1 individual IDs for group 1
     * @param individuals2 individual IDs for group 2
     * @param previousPhasingIds the previous phasing ids
     * @param annotationFieldThresholds1 the annotation field thresholds for group 1
     * @param annotationFieldThresholds2 the annotation field thresholds for group 2
     * @param warningFileWriter the warning file writer
     * @param synonym the synonym
     * @return the variant context
     * @throws Exception the exception
     */
    public VariantContext toVariantContext(MongoTemplate mongoTemplate, Collection<VariantRunData> runs, boolean exportVariantIDs, List<GenotypingSample> samplesToExport, Map<String, Integer> individualPositions, Collection<String> individuals1, Collection<String> individuals2, HashMap<Integer, Object> previousPhasingIds, HashMap<String, Float> annotationFieldThresholds1, HashMap<String, Float> annotationFieldThresholds2, FileWriter warningFileWriter, Comparable synonym) throws Exception
    {
        ArrayList<Genotype> genotypes = new ArrayList<Genotype>();
        String sRefAllele = knownAlleles.isEmpty() ? null : knownAlleles.iterator().next();

        HashMap<Integer, SampleGenotype> sampleGenotypes = new HashMap<>();
        HashSet<VariantRunData> runsWhereDataWasFound = new HashSet<>();

        // collect all genotypes from various runs for all individuals
        HashMap<String/*genotype code*/, LinkedHashSet<Integer/*sample*/>>[] individualGenotypes = new HashMap[individualPositions.size()];
        Integer knownAlleleCount = null;
        if (runs != null && !runs.isEmpty())
            for (VariantRunData run : runs) {
                for (GenotypingSample sample : samplesToExport) {
                    if (sRefAllele == null) {
                        knownAlleleCount = run.getKnownAlleles().size();
                        if (knownAlleleCount > 0)
                            sRefAllele = run.getKnownAlleles().iterator().next();
                    }
    
                    SampleGenotype sampleGenotype = run.getSampleGenotypes().get(sample.getId());
                    if (sampleGenotype == null || !gtPassesVcfAnnotationFilters(sample.getIndividual(), sampleGenotype, individuals1, annotationFieldThresholds1, individuals2, annotationFieldThresholds2))
                        continue;    // run contains no data for this sample, or its annotation values are below filter thresholds

                    // keep track of SampleGenotype and Run so we can have access to additional info later on
                    sampleGenotypes.put(sample.getId(), sampleGenotype);
                    runsWhereDataWasFound.add(run);

                    int nIndividualIndex = individualPositions.get(sample.getIndividual());
                    if (individualGenotypes[nIndividualIndex] == null)
                        individualGenotypes[nIndividualIndex] = new HashMap<>(1);
                    LinkedHashSet<Integer> samplesWithGivenGenotype = individualGenotypes[nIndividualIndex].get(sampleGenotype.getCode());
                    if (samplesWithGivenGenotype == null) {
                        samplesWithGivenGenotype = new LinkedHashSet<>(2);
                        individualGenotypes[nIndividualIndex].put(sampleGenotype.getCode(), samplesWithGivenGenotype);
                    }
                    samplesWithGivenGenotype.add(sample.getId());
                }
            }
        
        LinkedHashSet<Allele> variantAlleles = new LinkedHashSet<>(knownAlleleCount == null ? 4 : getKnownAlleles().size());
        variantAlleles.add(Allele.create(sRefAllele, true));
        HashMap<String, List<String>> genotypeStringCache = new HashMap<>(variantAlleles.size() * 2);

        for (Map.Entry<String, Integer> entry : individualPositions.entrySet()) {
            int nIndividualIndex = entry.getValue();
            HashMap<Object, Integer> genotypeCounts = new HashMap<>(2); // will help us to keep track of missing genotypes
                
            int highestGenotypeCount = 0;
            String mostFrequentGenotype = null;
            if (individualGenotypes[nIndividualIndex] != null) {
                if (individualGenotypes[nIndividualIndex].size() == 1)
                    mostFrequentGenotype = individualGenotypes[nIndividualIndex].keySet().iterator().next();
                else {
                    for (String gtCode : individualGenotypes[nIndividualIndex].keySet()) {
                        if (gtCode == null)
                            continue; /* skip missing genotypes */
    
                        int gtCount = individualGenotypes[nIndividualIndex].get(gtCode).size();
                        if (gtCount > highestGenotypeCount) {
                            highestGenotypeCount = gtCount;
                            mostFrequentGenotype = gtCode;
                        }
                        genotypeCounts.put(gtCode, gtCount);
                    }
                }
            }

            if (warningFileWriter != null && genotypeCounts.size() > 1) {
                List<Integer> reverseSortedGtCounts = genotypeCounts.values().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
                if (reverseSortedGtCounts.get(0) == reverseSortedGtCounts.get(1))
                    mostFrequentGenotype = null;
                warningFileWriter.write("- Dissimilar genotypes found for variant " + (synonym == null ? getVariantId() : synonym) + ", individual " + entry.getKey() + ". " + (mostFrequentGenotype == null ? "Exporting as missing data" : "Exporting most frequent: " + mostFrequentGenotype) + "\n");
            }
             
            if (mostFrequentGenotype == null)
                continue;    // no genotype for this individual

            Integer spId = individualGenotypes[nIndividualIndex].get(mostFrequentGenotype).iterator().next();    // any will do (although ideally we should make sure we export the best annotation values found)
            SampleGenotype sampleGenotype = sampleGenotypes.get(spId);

            Object currentPhId = sampleGenotype.getAdditionalInfo().get(GT_FIELD_PHASED_ID);
            boolean isPhased = currentPhId != null && currentPhId.equals(previousPhasingIds.get(spId));
            String gtCode = isPhased ? (String) sampleGenotype.getAdditionalInfo().get(GT_FIELD_PHASED_GT) : mostFrequentGenotype;
            List<String> alleles = genotypeStringCache.get(gtCode);
            if (alleles == null) {
                alleles = safelyGetAllelesFromGenotypeCode(gtCode, mongoTemplate);
                genotypeStringCache.put(gtCode, alleles);
            }

            ArrayList<Allele> individualAlleles = new ArrayList<>(alleles.size());
            previousPhasingIds.put(spId, currentPhId == null ? getVariantId() : currentPhId);
            if (alleles.size() == 0)
                continue;    /* skip this individual because there is no genotype for it */
            
            boolean fAllAllelesNoCall = !alleles.stream().filter(all -> !all.isEmpty()).findAny().isPresent();            
            for (String sAllele : alleles) {
                Allele allele = Allele.create(sAllele.length() == 0 ? (fAllAllelesNoCall ? Allele.NO_CALL_STRING : "<DEL>") : sAllele, sRefAllele.equals(sAllele) /* would be the same instance since wer'e using cached allele lists via genotypeStringCache */);
                if (!allele.isNoCall())
                    variantAlleles.add(allele);
                individualAlleles.add(allele);
            }

            GenotypeBuilder gb = new GenotypeBuilder(entry.getKey(), individualAlleles);
            if (individualAlleles.size() > 0)
            {
                gb.phased(isPhased);
                String genotypeFilters = (String) sampleGenotype.getAdditionalInfo().get(FIELD_FILTERS);
                if (genotypeFilters != null && genotypeFilters.length() > 0)
                    gb.filter(genotypeFilters);
                                
                List<String> alleleListAtImportTimeIfDifferentFromNow = null;
                for (String key : sampleGenotype.getAdditionalInfo().keySet())
                {
                    if (VCFConstants.GENOTYPE_ALLELE_DEPTHS.equals(key))
                    {
                        String ad = (String) sampleGenotype.getAdditionalInfo().get(key);
                        if (ad != null)
                        {
                            int[] adArray = Helper.csvToIntArray(ad);
                            if (knownAlleles.size() > adArray.length)
                            {
                                alleleListAtImportTimeIfDifferentFromNow = getKnownAlleles().subList(0, adArray.length);
                                adArray = VariantData.fixAdFieldValue(adArray, alleleListAtImportTimeIfDifferentFromNow, getKnownAlleles());
                            }
                            gb.AD(adArray);
                        }
                    }
                    else if (VCFConstants.DEPTH_KEY.equals(key) || VCFConstants.GENOTYPE_QUALITY_KEY.equals(key))
                    {
                        Integer value = (Integer) sampleGenotype.getAdditionalInfo().get(key);
                        if (value != null)
                        {
                            if (VCFConstants.DEPTH_KEY.equals(key))
                                gb.DP(value);
                            else
                                gb.GQ(value);
                        }
                    }
                    else if (VCFConstants.GENOTYPE_PL_KEY.equals(key) || VCFConstants.GENOTYPE_LIKELIHOODS_KEY.equals(key))
                    {
                        String fieldVal = (String) sampleGenotype.getAdditionalInfo().get(key);
                        if (fieldVal != null)
                        {
                            int[] plArray = VCFConstants.GENOTYPE_PL_KEY.equals(key) ? Helper.csvToIntArray(fieldVal) : GenotypeLikelihoods.fromGLField(fieldVal).getAsPLs();
                            if (alleleListAtImportTimeIfDifferentFromNow != null)
                                plArray = VariantData.fixPlFieldValue(plArray, individualAlleles.size(), alleleListAtImportTimeIfDifferentFromNow, getKnownAlleles());
                            gb.PL(plArray);
                        }
                    }
                    else if (!key.equals(VariantData.GT_FIELD_PHASED_GT) && !key.equals(VariantData.GT_FIELD_PHASED_ID) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE) && !key.equals(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME)) // exclude some internally created fields that we don't want to export
                        gb.attribute(key, sampleGenotype.getAdditionalInfo().get(key)); // looks like we have an extended attribute
                }                    
            }
            genotypes.add(gb.make());
        }

        VariantRunData run = runsWhereDataWasFound.size() == 1 ? runsWhereDataWasFound.iterator().next() : null;    // if there is not exactly one run involved then we do not export meta-data
        String source = run == null ? null : (String) run.getAdditionalInfo().get(FIELD_SOURCE);

        Long start = referencePosition == null ? null : referencePosition.getStartSite();
        Long stop = null;
        if (referencePosition != null) {
            if (referencePosition.getEndSite() != null)
                stop = referencePosition.getEndSite();
            else {
                if (sRefAllele == null) {
                    setKnownAlleles(mongoTemplate.findById(getVariantId(), VariantData.class).getKnownAlleles());
                    mongoTemplate.save(this);
                    sRefAllele = knownAlleles.iterator().next();
                }
                stop = start + sRefAllele.length() - 1;
            }
        }
        String chr = referencePosition == null ? null : referencePosition.getSequence();
        VariantContextBuilder vcb = new VariantContextBuilder(source != null ? source : FIELDVAL_SOURCE_MISSING, chr != null ? chr : "", start != null ? start : 0, stop != null ? stop : 0, variantAlleles);
        if (exportVariantIDs)
            vcb.id((synonym == null ? getVariantId() : synonym).toString());
        vcb.genotypes(genotypes);
        
        if (run != null) {
            Boolean fullDecod = (Boolean) run.getAdditionalInfo().get(FIELD_FULLYDECODED);
            vcb.fullyDecoded(fullDecod != null && fullDecod);
    
            String filters = (String) run.getAdditionalInfo().get(FIELD_FILTERS);
            if (filters != null)
                vcb.filters(filters.split(","));
            else
                vcb.filters(VCFConstants.UNFILTERED);
            
            Number qual = (Number) run.getAdditionalInfo().get(FIELD_PHREDSCALEDQUAL);
            if (qual != null)
                vcb.log10PError(qual.doubleValue() / -10.0D);
            
            for (String attrName : run.getAdditionalInfo().keySet())
                if (!VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME.equals(attrName) && !VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE.equals(attrName) && !specificallyTreatedAdditionalInfoFields.contains(attrName))
                    vcb.attribute(attrName, run.getAdditionalInfo().get(attrName));
        }
        VariantContext vc = vcb.make();
        return vc;
    }

    // tells whether applied filters imply to treat this genotype as missing data
    public static boolean gtPassesVcfAnnotationFilters(String individualName, SampleGenotype sampleGenotype, Collection<String> individuals1, HashMap<String, Float> annotationFieldThresholds, Collection<String> individuals2, HashMap<String, Float> annotationFieldThresholds2)
    {
        if ((annotationFieldThresholds == null || annotationFieldThresholds.isEmpty()) && annotationFieldThresholds2 == null)
            return true;

        List<HashMap<String, Float>> thresholdsToCheck = new ArrayList<HashMap<String, Float>>();
        if (!annotationFieldThresholds.isEmpty() && individuals1.contains(individualName))
            thresholdsToCheck.add(annotationFieldThresholds);
        if (!annotationFieldThresholds2.isEmpty() && individuals2.contains(individualName))
            thresholdsToCheck.add(annotationFieldThresholds2);
        
        for (HashMap<String, Float> someThresholdsToCheck : thresholdsToCheck)
            for (String annotationField : someThresholdsToCheck.keySet())
            {
                Integer annotationValue = null;
                try
                {
                    annotationValue = (Integer) sampleGenotype.getAdditionalInfo().get(annotationField);
                }
                catch (Exception ignored)
                {}
                if (annotationValue != null && annotationValue < someThresholdsToCheck.get(annotationField))
                    return false;
            }
        return true;
    }
    
    /* based on code from htsjdk.variant.variantcontext.VariantContext v2.14.3 */
    public static Type determinePolymorphicType(List<String> alleles) {
        
        switch ( alleles.size() ) {
            case 0:
                throw new IllegalStateException("Unexpected error: requested type of Variant with no alleles!");
            case 1:
                // note that this doesn't require a reference allele.  You can be monomorphic independent of having a
                // reference allele
                return Type.NO_VARIATION;
            default: {
                Type type = null;

                // do a pairwise comparison of all alleles against the reference allele
                for ( String allele : alleles ) {
                    if ( allele == alleles.get(0) )
                        continue;

                    // find the type of this allele relative to the reference
                    Type biallelicType = typeOfBiallelicVariant(alleles.get(0), allele);

                    // for the first alternate allele, set the type to be that one
                    if ( type == null ) {
                        type = biallelicType;
                    }
                    // if the type of this allele is different from that of a previous one, assign it the MIXED type and quit
                    else if ( biallelicType != type ) {
                        type = Type.MIXED;
                        break;
                    }
                }
                return type;
            }
        }

    }

    /* based on code from htsjdk.variant.variantcontext.VariantContext v2.14.3 */
    private static Type typeOfBiallelicVariant(String ref, String allele) {
        if ( "*".equals(ref) )
            throw new IllegalStateException("Unexpected error: encountered a record with a symbolic reference allele");

        if ( "*".equals(allele) )
            return Type.SYMBOLIC;

        if ( ref.length() == allele.length() ) {
            if ( allele.length() == 1 )
                return Type.SNP;
            else
                return Type.MNP;
        }

        // Important note: previously we were checking that one allele is the prefix of the other.  However, that's not an
        // appropriate check as can be seen from the following example:
        // REF = CTTA and ALT = C,CT,CA
        // This should be assigned the INDEL type but was being marked as a MIXED type because of the prefix check.
        // In truth, it should be absolutely impossible to return a MIXED type from this method because it simply
        // performs a pairwise comparison of a single alternate allele against the reference allele (whereas the MIXED type
        // is reserved for cases of multiple alternate alleles of different types).  Therefore, if we've reached this point
        // in the code (so we're not a SNP, MNP, or symbolic allele), we absolutely must be an INDEL.

        return Type.INDEL;

        // old incorrect logic:
        // if (oneIsPrefixOfOther(ref, allele))
        //     return Type.INDEL;
        // else
        //     return Type.MIXED;
    }

    abstract public String getVariantId();
}