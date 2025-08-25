package fr.cirad.mgdb.importing.parameters;

import java.io.File;
import java.net.URL;
import java.util.Map;

public class FlapjackImportParameters extends FileImportParameters {
    private File genotypeFile;

    public FlapjackImportParameters(String sModule, String sProject, String sRun, String sTechnology, Integer nPloidy, String assemblyName, Map<String, String> sampleToIndividualMap, boolean fSkipMonomorphic, int importMode, URL mainFileUrl, File genotypeFile) {
        super(sModule, sProject, sRun, sTechnology, nPloidy, assemblyName, sampleToIndividualMap, fSkipMonomorphic, importMode, mainFileUrl);
        this.genotypeFile = genotypeFile;
    }

    public File getGenotypeFile() {
        return genotypeFile;
    }

    public void setGenotypeFile(File genotypeFile) {
        this.genotypeFile = genotypeFile;
    }
}
