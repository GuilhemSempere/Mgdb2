package fr.cirad.mgdb.importing;

import java.util.HashMap;

public class DartInfo {

    private String alleleID;

    private String cloneID;

    private String alleleSequence;

    private String trimmedSequence;

    private String chrom;

    private Integer chromPos;

    private String alnCnt;

    private String alnEvalue;

    private String individualName;

    private String snp = null;

    private Integer snpPos = 0;

    private String callRate = null;

    private String oneRatioRef = null;

    private String oneRatioSNP = null;

    private String freqHomSnp = null;

    private String freqHomeRef = null;

    private String freqHets = null;

    private String picRef = null;

    private String picSNP = null;

    private String avgPic = null;

    private String avgCountRef = null;

    private String avgCountSNP = null;

    private String repAvg = null;

    private String strand = null;

    private HashMap<String, Integer> sampleGenotypes = new HashMap<String, Integer>();

    public DartInfo(String alleleID, String individualName) {
        this.alleleID = alleleID;
        this.individualName = individualName;
    }

    public DartInfo(String alleleID, String cloneID, String alleleSequence, String trimmedSequence, String chrom, Integer chromPos, String alnCnt, String alnEvalue, String individualName) {
        this.alleleID = alleleID;
        this.cloneID = cloneID;
        this.alleleSequence = alleleSequence;
        this.trimmedSequence = trimmedSequence;
        this.chrom = chrom;
        this.chromPos = chromPos;
        this.alnCnt = alnCnt;
        this.alnEvalue = alnEvalue;
        this.individualName = individualName;
    }

    public Integer getChromPos() {
        return chromPos;
    }

    public void setChromPos(Integer chromPos) {
        this.chromPos = chromPos;
    }

    public String getStrand() {
        return strand;
    }

    public void setStrand(String strand) {
        this.strand = strand;
    }

    public String getIndividualName() {
        return individualName;
    }

    public void setIndividualName(String individualName) {
        this.individualName = individualName;
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

    public String getAlnCnt() {
        return alnCnt;
    }

    public void setAlnCnt(String alnCnt) {
        this.alnCnt = alnCnt;
    }

    public String getAlnEvalue() {
        return alnEvalue;
    }

    public void setAlnEvalue(String alnEvalue) {
        this.alnEvalue = alnEvalue;
    }

    public String getChrom() {
        return chrom;
    }

    public void setChrom(String chrom) {
        this.chrom = chrom;
    }

    public String getCloneID() {
        return cloneID;
    }

    public void setCloneID(String cloneID) {
        this.cloneID = cloneID;
    }

    public String getTrimmedSequence() {
        return trimmedSequence;
    }

    public void setTrimmedSequence(String trimmedSequence) {
        this.trimmedSequence = trimmedSequence;
    }

    public Integer getSnpPos() {
        return snpPos;
    }

    public void setSnpPos(Integer snpPos) {
        this.snpPos = snpPos;
    }

    public String getAvgCountRef() {
        return avgCountRef;
    }

    public void setAvgCountRef(String avgCountRef) {
        this.avgCountRef = avgCountRef;
    }

    public String getAvgCountSNP() {
        return avgCountSNP;
    }

    public void setAvgCountSNP(String avgCountSNP) {
        this.avgCountSNP = avgCountSNP;
    }

    public String getAvgPic() {
        return avgPic;
    }

    public void setAvgPic(String avgPic) {
        this.avgPic = avgPic;
    }

    public String getCallRate() {
        return callRate;
    }

    public void setCallRate(String callRate) {
        this.callRate = callRate;
    }

    public String getFreqHets() {
        return freqHets;
    }

    public void setFreqHets(String freqHets) {
        this.freqHets = freqHets;
    }

    public String getFreqHomeRef() {
        return freqHomeRef;
    }

    public void setFreqHomeRef(String freqHomeRef) {
        this.freqHomeRef = freqHomeRef;
    }

    public String getFreqHomSnp() {
        return freqHomSnp;
    }

    public void setFreqHomSnp(String freqHomSnp) {
        this.freqHomSnp = freqHomSnp;
    }

    public String getOneRatioRef() {
        return oneRatioRef;
    }

    public void setOneRatioRef(String oneRatioRef) {
        this.oneRatioRef = oneRatioRef;
    }

    public String getOneRatioSNP() {
        return oneRatioSNP;
    }

    public void setOneRatioSNP(String oneRatioSNP) {
        this.oneRatioSNP = oneRatioSNP;
    }

    public String getPicRef() {
        return picRef;
    }

    public void setPicRef(String picRef) {
        this.picRef = picRef;
    }

    public String getPicSNP() {
        return picSNP;
    }

    public void setPicSNP(String picSNP) {
        this.picSNP = picSNP;
    }

    public String getRepAvg() {
        return repAvg;
    }

    public void setRepAvg(String repAvg) {
        this.repAvg = repAvg;
    }

    public String getSnp() {
        return snp;
    }

    public void setSnp(String snp) {
        this.snp = snp;
    }

    public HashMap<String, Integer> getSampleGenotypes() {
        return sampleGenotypes;
    }

    public void setSampleGenotypes(HashMap<String, Integer> sampleGenotypes) {
        this.sampleGenotypes = sampleGenotypes;
    }

    public Integer variantPos() {
        if (strand.equals("-")) {
            return chromPos - snpPos;
        }
        return chromPos + snpPos;
    }

    public Integer getStart() {
        return variantPos();
    }

    public Integer getStop() {
        return variantPos();
    }
}
