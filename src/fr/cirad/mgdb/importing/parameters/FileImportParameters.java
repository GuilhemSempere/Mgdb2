package fr.cirad.mgdb.importing.parameters;

import java.net.URL;
import java.util.Map;

public class FileImportParameters extends ImportParameters {
    private URL mainFileUrl;

    public FileImportParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode, URL mainFileUrl) {
        super(sModule, sProject, sRun, sTechnology, nPloidy, assemblyName, sampleToIndividualMap, fSkipMonomorphic, importMode);
        this.mainFileUrl = mainFileUrl;
    }

    public URL getMainFileUrl() {
        return mainFileUrl;
    }

    public void setMainFileUrl(URL mainFileUrl) {
        this.mainFileUrl = mainFileUrl;
    }
}
