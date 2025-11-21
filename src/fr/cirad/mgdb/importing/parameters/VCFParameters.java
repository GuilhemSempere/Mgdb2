package fr.cirad.mgdb.importing.parameters;

import java.net.URL;
import java.util.Map;

public class VCFParameters extends ImportParameters {
    private boolean fIsBCF;
    private URL mainFileUrl;

    public VCFParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode, boolean fIsBCF, URL mainFileUrl) {
        super(sModule, sProject, sRun, sTechnology, nPloidy, assemblyName, sampleToIndividualMap, fSkipMonomorphic, importMode);
        this.fIsBCF = fIsBCF;
        this.mainFileUrl = mainFileUrl;
    }

    public boolean isfIsBCF() {
        return fIsBCF;
    }

    public void setfIsBCF(boolean fIsBCF) {
        this.fIsBCF = fIsBCF;
    }

    public URL getMainFileUrl() {
        return mainFileUrl;
    }

    public void setMainFileUrl(URL mainFileUrl) {
        this.mainFileUrl = mainFileUrl;
    }
}
