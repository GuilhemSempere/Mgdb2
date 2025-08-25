package fr.cirad.mgdb.importing.parameters;

import java.io.File;
import java.net.URL;
import java.util.Map;

public class PlinkParameters extends FileImportParameters {
    private File pedFile;
    private boolean fCheckConsistencyBetweenSynonyms;

    public PlinkParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode, URL mainFileUrl, File pedFile, boolean fCheckConsistencyBetweenSynonyms) {
        super(sModule, sProject, sRun, sTechnology, nPloidy, assemblyName, sampleToIndividualMap, fSkipMonomorphic, importMode, mainFileUrl);
        this.fCheckConsistencyBetweenSynonyms = fCheckConsistencyBetweenSynonyms;
        this.pedFile = pedFile;
    }

    public File getPedFile() {
        return pedFile;
    }

    public void setPedFile(File pedFile) {
        this.pedFile = pedFile;
    }

    public boolean isfCheckConsistencyBetweenSynonyms() {
        return fCheckConsistencyBetweenSynonyms;
    }

    public void setfCheckConsistencyBetweenSynonyms(boolean fCheckConsistencyBetweenSynonyms) {
        this.fCheckConsistencyBetweenSynonyms = fCheckConsistencyBetweenSynonyms;
    }
}
