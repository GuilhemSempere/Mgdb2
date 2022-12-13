package jhi.brapi.api.germplasm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jhi.brapi.api.germplasm.BrapiGermplasmDonor;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiGermplasm {
	
    static public final HashMap<String, String> germplasmFields = new HashMap<>();

    private String germplasmDbId;
    private String defaultDisplayName;
    private String germplasmName;
    private String accessionNumber;
    private String germplasmPUI;
    private String pedigree;
    private String seedSource;
    private List<String> synonyms;
    private String commonCropName;
    private String instituteCode;
    private String instituteName;
    private String biologicalStatusOfAccessionCode;
    private String countryOfOriginCode;
    private List<String> typeOfGermplasmStorageCode;
    private String genus;
    private String species;
    private List<Map<String, String>> taxonIds;
    private String speciesAuthority;
    private String subtaxa;
    private String subtaxaAuthority;
    private List<BrapiGermplasmDonor> donors;
    private String acquisitionDate;
    
    static {
//    	germplasmFields.put("germplasmname", "germplasmName");
    	germplasmFields.put("defaultdisplayname", "defaultDisplayName");
    	germplasmFields.put("accessionnumber", "accessionNumber");
    	germplasmFields.put("germplasmpui", "germplasmPUI");
    	germplasmFields.put("pedigree", "pedigree");
    	germplasmFields.put("seedsource", "seedSource");
    	germplasmFields.put("commoncropname", "commonCropName");
    	germplasmFields.put("institutecode", "instituteCode");
    	germplasmFields.put("institutename", "instituteName");
    	germplasmFields.put("biologicalstatusofaccessioncode", "biologicalStatusOfAccessionCode");
    	germplasmFields.put("countryoforigincode", "countryOfOriginCode");
    	germplasmFields.put("typeofgermplasmstoragecode", "typeOfGermplasmStorageCode");
    	germplasmFields.put("genus", "genus");
    	germplasmFields.put("species", "species");
    	germplasmFields.put("speciesauthority", "speciesAuthority");
    	germplasmFields.put("subtaxa", "subtaxa");
    	germplasmFields.put("subtaxaauthority", "subtaxaAuthority");
    	germplasmFields.put("acquisitiondate", "acquisitionDate");
    }

    public String getGermplasmDbId() {
        return this.germplasmDbId;
    }

    public void setGermplasmDbId(String germplasmDbId) {
        this.germplasmDbId = germplasmDbId;
    }

    public String getDefaultDisplayName() {
        return this.defaultDisplayName;
    }

    public void setDefaultDisplayName(String defaultDisplayName) {
        this.defaultDisplayName = defaultDisplayName;
    }

    public String getGermplasmName() {
        return this.germplasmName;
    }

    public void setGermplasmName(String germplasmName) {
        this.germplasmName = germplasmName;
    }

    public String getAccessionNumber() {
        return this.accessionNumber;
    }

    public void setAccessionNumber(String accessionNumber) {
        this.accessionNumber = accessionNumber;
    }

    public String getGermplasmPUI() {
        return this.germplasmPUI;
    }

    public void setGermplasmPUI(String germplasmPUI) {
        this.germplasmPUI = germplasmPUI;
    }

    public String getPedigree() {
        return this.pedigree;
    }

    public void setPedigree(String pedigree) {
        this.pedigree = pedigree;
    }

    public String getSeedSource() {
        return this.seedSource;
    }

    public void setSeedSource(String seedSource) {
        this.seedSource = seedSource;
    }

    public List<String> getSynonyms() {
        return this.synonyms;
    }

    public void setSynonyms(List<String> synonyms) {
        this.synonyms = synonyms;
    }

    public String getCommonCropName() {
        return this.commonCropName;
    }

    public void setCommonCropName(String commonCropName) {
        this.commonCropName = commonCropName;
    }

    public String getInstituteCode() {
        return this.instituteCode;
    }

    public void setInstituteCode(String instituteCode) {
        this.instituteCode = instituteCode;
    }

    public String getInstituteName() {
        return this.instituteName;
    }

    public void setInstituteName(String instituteName) {
        this.instituteName = instituteName;
    }

    public String getBiologicalStatusOfAccessionCode() {
        return this.biologicalStatusOfAccessionCode;
    }

    public void setBiologicalStatusOfAccessionCode(String biologicalStatusOfAccessionCode) {
        this.biologicalStatusOfAccessionCode = biologicalStatusOfAccessionCode;
    }

    public String getCountryOfOriginCode() {
        return this.countryOfOriginCode;
    }

    public void setCountryOfOriginCode(String countryOfOriginCode) {
        this.countryOfOriginCode = countryOfOriginCode;
    }

    public List<String> getTypeOfGermplasmStorageCode() {
        return this.typeOfGermplasmStorageCode;
    }

    public void setTypeOfGermplasmStorageCode(List<String> typeOfGermplasmStorageCode) {
        this.typeOfGermplasmStorageCode = typeOfGermplasmStorageCode;
    }

    public String getGenus() {
        return this.genus;
    }

    public void setGenus(String genus) {
        this.genus = genus;
    }

    public String getSpecies() {
        return this.species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    public List<Map<String, String>> getTaxonIds() {
        return this.taxonIds;
    }

    public void setTaxonIds(List<Map<String, String>> taxonIds) {
        this.taxonIds = taxonIds;
    }

    public String getSpeciesAuthority() {
        return this.speciesAuthority;
    }

    public void setSpeciesAuthority(String speciesAuthority) {
        this.speciesAuthority = speciesAuthority;
    }

    public String getSubtaxa() {
        return this.subtaxa;
    }

    public void setSubtaxa(String subtaxa) {
        this.subtaxa = subtaxa;
    }

    public String getSubtaxaAuthority() {
        return this.subtaxaAuthority;
    }

    public void setSubtaxaAuthority(String subtaxaAuthority) {
        this.subtaxaAuthority = subtaxaAuthority;
    }

    public List<BrapiGermplasmDonor> getDonors() {
        return this.donors;
    }

    public void setDonors(List<BrapiGermplasmDonor> donors) {
        this.donors = donors;
    }

    public String getAcquisitionDate() {
        return this.acquisitionDate;
    }

    public void setAcquisitionDate(String acquisitionDate) {
        this.acquisitionDate = acquisitionDate;
    }
}

