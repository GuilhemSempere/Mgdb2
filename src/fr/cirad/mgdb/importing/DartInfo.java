package fr.cirad.mgdb.importing;

import java.util.ArrayList;
import java.util.HashMap;

public class DartInfo {

    private String alleleID;

    private String alleleSequence;

    private String trimmedSequence;

    private String chrom;

    private Integer chromPos;

//    private String individualName;

    private Integer snpPos = 0;

    private String strand = "+";

    private float callRate;

    private float freqHomRef;

    private float freqHomSnp;

    private float freqHets;

    private String[] genotypes = null;

    private String[] alleles = new String[2];

    private String[] sampleIDs = new String[]{};

    public DartInfo(String alleleID/*, String individualName*/) {
        this.alleleID = alleleID;
//        this.individualName = individualName;
    }

    public DartInfo(String alleleID, String alleleSequence, String trimmedSequence, String chrom, Integer chromPos, String individualName, float callRate, float freqHomRef, float freqHets) {
        this.alleleID = alleleID;
        this.alleleSequence = alleleSequence;
        this.trimmedSequence = trimmedSequence;
        this.chrom = chrom;
        this.chromPos = chromPos;
//        this.individualName = individualName;
        this.callRate = callRate;
        this.freqHomRef = freqHomRef;
        this.freqHets = freqHets;
    }


    public String getAlleleID() {
        return alleleID;
    }

    public void setAlleleID(String alleleID) {
        this.alleleID = alleleID;
    }


    public String getAlleleSequence() {
        return alleleSequence;
    }

    public void setAlleleSequence(String alleleSequence) {
        this.alleleSequence = alleleSequence;
    }


    public String getTrimmedSequence() {
        return trimmedSequence;
    }

    public void setTrimmedSequence(String trimmedSequence) {
        this.trimmedSequence = trimmedSequence;
    }


    public String getChrom() {
        return chrom;
    }

    public void setChrom(String chrom) {
        if (chrom.isEmpty() || chrom.equals("0"))
            chrom = "Un";
        if (chrom.contains(" "))
            chrom = chrom.split(" ")[0];
        this.chrom = chrom;
    }


    public Integer getChromPos() {
        return chromPos;
    }

    public void setChromPos(Integer chromPos) {
        this.chromPos = chromPos;
    }


//    public String getIndividualName() {
//        return individualName;
//    }
//
//    public void setIndividualName(String individualName) {
//        this.individualName = individualName;
//    }

    public Integer getSnpPos() {
        return snpPos;
    }

    public void setSnpPos(Integer snpPos) {
        this.snpPos = snpPos;
    }


    public String getStrand() {
        return strand;
    }

    public void setStrand(String strand) {
        this.strand = strand;
    }


    public float getCallRate() {
        return callRate;
    }

    public void setCallRate(float callRate) {
        this.callRate = callRate;
    }


    public float getFreqHomRef() {
        return freqHomRef;
    }

    public void setFreqHomRef(float freqHomRef) {
        this.freqHomRef = freqHomRef;
    }


    public float getFreqHomSnp() {
        return freqHomSnp;
    }

    public void setFreqHomSnp(float freqHomSnp) {
        this.freqHomSnp = freqHomSnp;
    }


    public float getFreqHets() {
        return freqHets;
    }

    public void setFreqHets(float freqHets) {
        this.freqHets = freqHets;
    }

    public String[] getGenotypes() {
        return genotypes;
    }

    public void setGenotypes(String[] genotypes) {
        this.genotypes = genotypes;
    }


    public String[] getAlleles() {
        return alleles;
    }

    public void setAlleles(String[] alleles) {
        this.alleles = alleles;
    }


    public String[] getSampleIDs() {
        return sampleIDs;
    }

    public void setSampleIDs(String[] sampleIDs) {
        this.sampleIDs = sampleIDs;
    }


    public Integer variantPos() {
        if (strand.equals("-") || strand.equals("Minus")) {
            return chromPos - snpPos;
        }
        return chromPos + snpPos;
    }

    public Integer getStart() {
        return variantPos();
    }

    public Integer getEnd() {
        return variantPos();
    }
}
