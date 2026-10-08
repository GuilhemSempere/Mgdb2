package fr.cirad.mgdb.importing.parameters;

import java.net.URL;
import java.util.Map;

public class DartImportParameters extends FileImportParameters {
    private URL countsFileUrl;

    public DartImportParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode, URL genotypeFileUrl, URL countsFileUrl) {
        super(sModule, sProject, sRun, sTechnology, nPloidy, assemblyName, sampleToIndividualMap, fSkipMonomorphic, importMode, genotypeFileUrl);
        this.countsFileUrl = countsFileUrl;
    }

    public URL getCountsFileUrl() {
    	return countsFileUrl;
    }
    
    public void setCountsFileUrl(URL countsFileUrl) {
    	this.countsFileUrl = countsFileUrl;
    }
}