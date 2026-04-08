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
package fr.cirad.mgdb.model.mongo.maintypes;

import fr.cirad.tools.mongo.AutoIncrementCounter;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for computing and persisting numeric genotype codes.
 * Stores mapping between numeric genotype encoding and equivalent VCF-like genotype encoding.
 *
 * Encoding rules:
 * - homozygous >= 0, heterozygous < 0
 * - biallelic codes reserved: -9999 to 9999
 *     code = sign x (ploidy x 100 + altCount)
 *     het: negative, hom: positive
 *     supports ploidy up to 99
 * - multiallelic codes outside reserved range:
 *     het: sequential from -10000 downward
 *     hom: sequential from 10000 upward
 */
public class GenotypeCodeManager {

    public final static String FIELDNAME_DECODED_GENOTYPE = "gt";
    private static final String HET_COUNTER_KEY = "genotypeCodeHet";
    private static final String HOM_COUNTER_KEY = "genotypeCodeHom";
    private static final int BIALLELIC_LIMIT = 9999;
    private static final int MULTIALLELIC_HET_START = -10000;
    private static final int MULTIALLELIC_HOM_START = 10000;

    /**
     * Computes the numeric genotype code for the given alleles, saves it to the
     * genotypeCodes collection if new, and returns the code.
     *
     * @param alleles        the alleles for this genotype e.g. ["A", "T"]
     * @param alleleIndexMap map from allele string to its index in knownAlleles
     * @param mongoTemplate  the MongoTemplate to use
     * @return the numeric genotype code
     */
    public static synchronized Integer createGenotypeEncoding(List<String> alleles, Map<String, Integer> alleleIndexMap, MongoTemplate mongoTemplate) {

        if (alleles.get(0)==null)
            return null;
        List<Integer> alleleIndices = alleles.stream()
                .map(alleleIndexMap::get)
                .collect(Collectors.toList());

        boolean isHet = alleleIndices.stream().distinct().count() > 1;
        String gtKey = alleleIndices.stream().sorted().map(Object::toString).collect(Collectors.joining("/"));

        int code;

        if (alleleIndexMap.size() <= 2) {
            // biallelic: code = sign x (ploidy x 100 + altCount)
            int ploidy = alleles.size();
            int altCount = (int) alleleIndices.stream().filter(idx -> idx > 0).count();
            int value = ploidy * 100 + altCount;
            code = isHet ? -value : value;
        } else if (!isHet) {
            code = findOrCreateHomCode(gtKey, mongoTemplate);
        } else {
            code = findOrCreateHetCode(gtKey, mongoTemplate);
        }

        if (alleleIndexMap.size() > 2)
            if (!mongoTemplate.exists(new Query(Criteria.where("_id").is(code)), GenotypeCode.class))
                mongoTemplate.save(new GenotypeCode(code, gtKey));

        return code;
    }

    private static int findOrCreateHetCode(String gtKey, MongoTemplate mongoTemplate) {
        GenotypeCode existing = mongoTemplate.findOne(new Query(Criteria.where(FIELDNAME_DECODED_GENOTYPE).is(gtKey)), GenotypeCode.class);
        if (existing != null)
            return existing.getId();

        AutoIncrementCounter counter = mongoTemplate.findAndModify(
                new Query(Criteria.where("_id").is(HET_COUNTER_KEY)),
                new Update().inc("seq", -1),
                FindAndModifyOptions.options().returnNew(true),
                AutoIncrementCounter.class);

        if (counter != null)
            return counter.getSeq();

        mongoTemplate.save(new AutoIncrementCounter(HET_COUNTER_KEY, MULTIALLELIC_HET_START));
        return MULTIALLELIC_HET_START;
    }

    private static int findOrCreateHomCode(String gtKey, MongoTemplate mongoTemplate) {
        GenotypeCode existing = mongoTemplate.findOne(
                new Query(Criteria.where(FIELDNAME_DECODED_GENOTYPE).is(gtKey)), GenotypeCode.class);
        if (existing != null)
            return existing.getId();

        AutoIncrementCounter counter = mongoTemplate.findAndModify(
                new Query(Criteria.where("_id").is(HOM_COUNTER_KEY)),
                new Update().inc("seq", 1),
                FindAndModifyOptions.options().returnNew(true),
                AutoIncrementCounter.class);

        if (counter != null)
            return counter.getSeq();

        mongoTemplate.save(new AutoIncrementCounter(HOM_COUNTER_KEY, MULTIALLELIC_HOM_START));
        return MULTIALLELIC_HOM_START;
    }

    /**
     * Decodes a numeric genotype code back to a VCF-style allele index string e.g. "0/1".
     *
     * Biallelic (abs(code) <= 9999): decoded arithmetically from ploidy * 100 + altCount.
     * Multiallelic (abs(code) >= 10000): looked up in genotypeCodes collection by _id = code.
     *
     * @param code          the numeric genotype code
     * @param mongoTemplate the MongoTemplate to use for multiallelic lookup
     * @return the VCF-style genotype string e.g. "0/1/1"
     */
    public static String decodeGenotypeCode(Integer code, MongoTemplate mongoTemplate) {

        if (code == null)
            return null;

        int abs = Math.abs(code);

        if (abs <= BIALLELIC_LIMIT) {
            int ploidy = abs / 100;
            int altCount = abs % 100;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ploidy; i++) {
                if (i > 0) sb.append("/");
                sb.append(i < (ploidy - altCount) ? "0" : "1");
            }
            return sb.toString();
        }

        GenotypeCode gc = mongoTemplate.findById(code, GenotypeCode.class);
        if (gc == null)
            throw new IllegalStateException("No GenotypeCode found for code: " + code);
        return gc.getGt();
    }
}