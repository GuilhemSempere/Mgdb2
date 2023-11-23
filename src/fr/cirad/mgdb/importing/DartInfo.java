package fr.cirad.mgdb.importing;

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
}
