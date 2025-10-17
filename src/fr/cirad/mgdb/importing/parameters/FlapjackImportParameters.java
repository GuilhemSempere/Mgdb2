package fr.cirad.mgdb.importing.parameters;

import java.net.URL;
import java.util.Map;

public class FlapjackImportParameters extends FileImportParameters {
    private URL mapFileUrl;

    public FlapjackImportParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode, URL genotypeFileUrl, URL mapFileUrl) {
        super(sModule, sProject, sRun, sTechnology, nPloidy, assemblyName, sampleToIndividualMap, fSkipMonomorphic, importMode, genotypeFileUrl);
        this.mapFileUrl = mapFileUrl;
    }

    public URL getMapFileUrl() {
        return mapFileUrl;
    }

    public void setMapFileUrl(URL mapFileUrl) {
        this.mapFileUrl = mapFileUrl;
    }
}