package fr.cirad.mgdb.importing.parameters;

import java.util.Map;

public class ImportParameters {
    private String sModule;
    private String sProject;
    private String sRun;
    private String sTechnology;
    private Integer nPloidy;
    private String assemblyName;
    private Map<String, String> sampleToIndividualMap;
    private boolean fSkipMonomorphic;
    private int importMode;

    public ImportParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode) {
        this.sModule = sModule;
        this.sProject = sProject;
        this.sRun = sRun;
        this.sTechnology = sTechnology;
        this.nPloidy = nPloidy;
        this.assemblyName = assemblyName;
        this.sampleToIndividualMap = sampleToIndividualMap;
        this.fSkipMonomorphic = fSkipMonomorphic;
        this.importMode = importMode;
    }

    public String getsModule() {
        return sModule;
    }

    public void setsModule(String sModule) {
        this.sModule = sModule;
    }

    public String getsProject() {
        return sProject;
    }

    public void setsProject(String sProject) {
        this.sProject = sProject;
    }

    public String getsRun() {
        return sRun;
    }

    public void setsRun(String sRun) {
        this.sRun = sRun;
    }

    public String getsTechnology() {
        return sTechnology;
    }

    public void setsTechnology(String sTechnology) {
        this.sTechnology = sTechnology;
    }

    public Integer getnPloidy() {
        return nPloidy;
    }

    public void setnPloidy(Integer nPloidy) {
        this.nPloidy = nPloidy;
    }

    public String getAssemblyName() {
        return assemblyName;
    }

    public void setAssemblyName(String assemblyName) {
        this.assemblyName = assemblyName;
    }

    public Map<String, String> getSampleToIndividualMap() {
        return sampleToIndividualMap;
    }

    public void setSampleToIndividualMap(Map<String, String> sampleToIndividualMap) {
        this.sampleToIndividualMap = sampleToIndividualMap;
    }

    public boolean isfSkipMonomorphic() {
        return fSkipMonomorphic;
    }

    public void setfSkipMonomorphic(boolean fSkipMonomorphic) {
        this.fSkipMonomorphic = fSkipMonomorphic;
    }

    public int getImportMode() {
        return importMode;
    }

    public void setImportMode(int importMode) {
        this.importMode = importMode;
    }
}
