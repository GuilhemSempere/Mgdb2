package fr.cirad.mgdb.importing.parameters;

import java.net.URL;
import java.util.Map;

public class PlinkImportParameters extends FileImportParameters {
    private URL mapFileUrl;
    private boolean fCheckConsistencyBetweenSynonyms;

    public PlinkImportParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode, URL mainFileUrl, URL mapFileUrl, boolean fCheckConsistencyBetweenSynonyms) {
        super(sModule, sProject, sRun, sTechnology, nPloidy, assemblyName, sampleToIndividualMap, fSkipMonomorphic, importMode, mainFileUrl);
        this.fCheckConsistencyBetweenSynonyms = fCheckConsistencyBetweenSynonyms;
        this.mapFileUrl = mapFileUrl;
    }

    public URL getMapFileUrl() {
        return mapFileUrl;
    }

    public void setMapFileUrl(URL mapFileUrl) {
        this.mapFileUrl = mapFileUrl;
    }

    public boolean isfCheckConsistencyBetweenSynonyms() {
        return fCheckConsistencyBetweenSynonyms;
    }

    public void setfCheckConsistencyBetweenSynonyms(boolean fCheckConsistencyBetweenSynonyms) {
        this.fCheckConsistencyBetweenSynonyms = fCheckConsistencyBetweenSynonyms;
    }
}
