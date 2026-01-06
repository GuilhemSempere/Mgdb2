package fr.cirad.mgdb.importing.parameters;

import java.util.Map;

public class ImportParameters {
    private String module;
    private String sProject;
    private String sRun;
    private String sTechnology;
    private Integer nPloidy;
    private String assemblyName;
    private Map<String, String> sampleToIndividualMap;
    private boolean fSkipMonomorphic;
    private int importMode;

    public ImportParameters(String module, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode) {
        this.module = module;
        this.sProject = sProject;
        this.sRun = sRun;
        this.sTechnology = sTechnology;
        this.nPloidy = nPloidy;
        this.assemblyName = assemblyName;
        this.sampleToIndividualMap = sampleToIndividualMap;
        this.fSkipMonomorphic = fSkipMonomorphic;
        this.importMode = importMode;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

    public String getProject() {
        return sProject;
    }

    public void setProject(String sProject) {
        this.sProject = sProject;
    }

    public String getRun() {
        return sRun;
    }

    public void setRun(String sRun) {
        this.sRun = sRun;
    }

    public String getTechnology() {
        return sTechnology;
    }

    public void setTechnology(String sTechnology) {
        this.sTechnology = sTechnology;
    }

    public Integer getPloidy() {
        return nPloidy;
    }

    public void setPloidy(Integer nPloidy) {
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

    public boolean isSkipMonomorphic() {
        return fSkipMonomorphic;
    }

    public void setSkipMonomorphic(boolean fSkipMonomorphic) {
        this.fSkipMonomorphic = fSkipMonomorphic;
    }

    public int getImportMode() {
        return importMode;
    }

    public void setImportMode(int importMode) {
        this.importMode = importMode;
    }
}
