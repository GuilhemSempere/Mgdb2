/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2019, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package fr.cirad.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Field;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class Helper.
 */
public class Helper {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(Helper.class);

    /**
     * The md.
     */
    static MessageDigest md = null;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Unable to find MD5 algorithm", e);
        }
    }

	static public double choose(double n, double k) {
        double r = 1;
        for (double i = 0; i < k; i++) {
            r *= (n - i); 
            r /= (i + 1); 
        }  
        return r;
    }

    /**
     * Convert string array to number array.
     *
     * @param stringArray the string array
     * @return the number[]
     */
    public static Number[] convertStringArrayToNumberArray(String[] stringArray) {
        if (stringArray == null || stringArray.length == 0) {
            return new Number[0];
        }

        Number[] result = new Number[stringArray.length];
        for (int i = 0; i < stringArray.length; i++) {
            result[i] = Double.parseDouble(stringArray[i]);
        }
        return result;
    }

    /**
     * Array to csv.
     *
     * @param separator the separator
     * @param array the array
     * @return the string
     */
    public static String arrayToCsv(String separator, int[] array) {
        if (array == null) {
            return null;
        }

        StringBuilder result = new StringBuilder("");
        for (int val : array) {
            result.append(result.length() == 0 ? "" : separator).append(val);
        }
        return result.toString();
    }

    /**
     * Array to csv.
     *
     * @param separator the separator
     * @param array the array
     * @return the string
     */
    public static String arrayToCsv(String separator, Collection<?> array) {
        if (array == null) {
            return null;
        }

        StringBuilder result = new StringBuilder("");
        for (Object val : array) {
            result.append(result.length() == 0 ? "" : separator).append(val);
        }
        return result.toString();
    }

    /**
     * Csv to int array.
     *
     * @param csvString the csv string
     * @return the int[]
     */
    public static int[] csvToIntArray(String csvString) {
        if (csvString == null) {
            return new int[0];
        }

        String[] splittedString = csvString.split(",");
        int[] result = new int[splittedString.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = Integer.parseInt(splittedString[i]);
        }
        return result;
    }

    /**
     * Split.
     *
     * @param stringToSplit the string to split
     * @param delimiter the delimiter
     * @return the list
     */
    public static List<String> split(String stringToSplit, String delimiter) {
        List<String> splittedString = new ArrayList<>();
        if (stringToSplit != null) {
            int pos = 0, end;
            while ((end = stringToSplit.indexOf(delimiter, pos)) >= 0) {
                splittedString.add(stringToSplit.substring(pos, end));
                pos = end + delimiter.length();
            }
            splittedString.add(stringToSplit.substring(pos));
        }
        return splittedString;
    }

    /**
     * Extract columns from csv.
     *
     * @param f the file
     * @param sSeparator the separator
     * @param returnedColumnIndexes the returned column indexes
     * @param nNHeaderLinesToSkip the n n header lines to skip
     * @return the list
     * @throws Exception the exception
     */
    public static List<List<String>> extractColumnsFromCsv(File f, String sSeparator, int[] returnedColumnIndexes, int nNHeaderLinesToSkip) throws Exception {
        List<List<String>> result = new ArrayList<>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(f));
            for (int i = 0; i < nNHeaderLinesToSkip; i++) {
                br.readLine();	// skip this line
            }
            String sLine = br.readLine();
            if (sLine != null) {
                sLine = sLine.trim();
            }
            int nLineCount = 0;
            do {
                List<String> splittedLine = split(sLine, sSeparator);
                if (returnedColumnIndexes == null) {
                    result.add(splittedLine);	// we return the while line
                } else // we return only some cells in the line
                {
                    for (int j = 0; j < returnedColumnIndexes.length; j++) {
                        if (splittedLine.size() < returnedColumnIndexes[j] - 1) {
                            throw new Exception("Unable to find column n." + returnedColumnIndexes[j] + " in line: " + sLine);
                        } else {
                            if (nLineCount == 0) {
                                result.add(new ArrayList<String>());
                            }
                            result.get(j).add(splittedLine.get(returnedColumnIndexes[j]));
                        }
                    }
                }

                sLine = br.readLine();
                if (sLine != null) {
                    sLine = sLine.trim();
                }
                nLineCount++;
            } while (sLine != null);
        } finally {
            if (br != null) {
                br.close();
            }
        }
        return result;
    }

    /**
     * Gets the count for key.
     *
     * @param keyToCountMap the key to count map
     * @param sampleId the key
     * @return the count for key
     */
    public static int getCountForKey(HashMap<Object, Integer> keyToCountMap, Object key) {
        Integer gtCount = keyToCountMap.get(key);
        if (gtCount == null) {
            gtCount = 0;
        }
        return gtCount;
    }

    /**
     * Convert to m d5.
     *
     * @param string the string
     * @return md5 checksum 32 char long
     */
    public static String convertToMD5(String string) {
        if (md == null) {
            return string;
        }

        byte[] messageDigest = md.digest(string.getBytes());
        BigInteger number = new BigInteger(1, messageDigest);
        String md5String = number.toString(16);
        // Now we need to zero pad it if you actually want the full 32 chars.
        while (md5String.length() < 32) {
            md5String = "0" + md5String;
        }
        return md5String;
    }

    /**
     * Null to empty string.
     *
     * @param s the string
     * @return the object
     */
    public static String nullToEmptyString(Object s) {
        return s == null ? "" : s.toString();
    }

    /**
     * 
     * @param o the potential string
     * @return true if the passed object is null or an empty string (after trimming)
     */
    public static boolean isNullOrEmptyString(Object o) {
        return o == null || (o instanceof String && ((String) o).trim().isEmpty());
    }


    /**
     * Read possibly nested field.
     *
     * @param doc the record
     * @param fieldPath the field path
     * @param listFieldSeparator separator to use for list fields
     * @return the object
     */
    public static Object readPossiblyNestedField(Document doc, String fieldPath, String listFieldSeparator) {
    	Document slidingRecord = doc;
        String[] splitFieldName = fieldPath.split("\\.");
        Object o = null, result;
        for (String s : splitFieldName) {
            o = slidingRecord.get(s);
            if (o != null && Document.class.isAssignableFrom(o.getClass())) {
                slidingRecord = ((Document) o);
            }
        }
        if (o != null && List.class.isAssignableFrom(o.getClass())) {
            result = new ArrayList<>();
            for (Object o2 : ((List) o)) {
                if (o2 != null && List.class.isAssignableFrom(o2.getClass())) {
                    ((ArrayList<Object>) result).addAll(((List) o2));
                } else {
                    ((ArrayList<Object>) result).add(o2);
                }
            }
            result = StringUtils.join(((ArrayList<Object>) result), listFieldSeparator);
        } else {
            result = o;
        }

        if (result == null) {
            result = "";
        }

        return result;
    }

    /**
     * convert a byte[] to string
     *
     * @param b
     * @return
     */
    public static String bytesToHex(byte[] b) {

        char hexDigit[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
            'b', 'c', 'd', 'e', 'f'};
        StringBuilder buf = new StringBuilder();

        for (int j = 0; j < b.length; j++) {
            buf.append(hexDigit[(b[j] >> 4) & 0x0f]);
            buf.append(hexDigit[b[j] & 0x0f]);
        }
        return buf.toString();
    }
    
    public static long estimDocCount(MongoTemplate mongoTemplate, String collName) {
    	return mongoTemplate.getCollection(collName).estimatedDocumentCount();
    }
    
    public static long estimDocCount(MongoTemplate mongoTemplate, Class documentClass) {
    	return estimDocCount(mongoTemplate, mongoTemplate.getCollectionName(documentClass));
    }
    
    public static long estimDocCount(String sModule,String collName) {
    	return estimDocCount(MongoTemplateManager.get(sModule), collName);
    }
    
    public static long estimDocCount(String sModule, Class documentClass) {
    	return estimDocCount(MongoTemplateManager.get(sModule), documentClass);
    }

    public static LinkedHashMap<String, String> getDocFieldNamesFromFieldAnnotationValues(Class clazz, Collection<String> annotationValues) {
    	LinkedHashMap<String, String> result = new LinkedHashMap<>();
		for (java.lang.reflect.Field field : clazz.getDeclaredFields()) {
			Field fieldAnnotation = field.getAnnotation(Field.class);
			if (fieldAnnotation != null && annotationValues.contains(fieldAnnotation.value()))
				result.put(field.getName(), field.getType().getSimpleName());
		}
		return result;
    }

    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    public static HashMap<Integer /*project*/, List<String /*runs*/>> getRunsByProjectInSampleCollection(Collection<GenotypingSample> samples) {
		HashMap<Integer, List<String>> runsByProject = new HashMap<>();
		for (String projectAndRun : samples.stream().map(sp -> sp.getProjectId() + "§" + sp.getRun()).distinct().collect(Collectors.toList())) {
			String[] separateIDs = projectAndRun.split("§");
			int projId = Integer.parseInt(separateIDs[0]);
			List<String> projectRuns = runsByProject.get(projId);
			if (projectRuns == null) {
				projectRuns = new ArrayList<>();
				runsByProject.put(projId, projectRuns);
			}
			projectRuns.add(separateIDs[1]);
		}
		return runsByProject;
    }
    
    static public void convertIdFiltersToRunFormat(Collection<BasicDBList> filters) {
        for (BasicDBList query: filters) {
            for (Object filter : query) {
                if (filter instanceof BasicDBObject) {
                    if (((BasicDBObject) filter).containsField("_id"))
                        ((BasicDBObject) filter).append("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, ((BasicDBObject) filter).remove("_id"));
                }
                else
                    if (((Document) filter).containsKey("_id"))
                        ((Document) filter).append("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, ((BasicDBObject) filter).remove("_id"));
            }
        }
    }
    
    static public <T> List<Collection<T>> evenlySplitCollection(Collection<T> collection, int nChunkCount) {
        int nChunkSize = (int) Math.ceil((float) collection.size() / nChunkCount), nCounter = 0;
        List<Collection<T>> splitCollection = new ArrayList<>(nChunkCount);
        Collection<T> currentChunk = null;
        for (T variantRuns : collection) {
            if (nCounter++ % nChunkSize == 0) {
                currentChunk = new ArrayList<>();
                splitCollection.add(currentChunk);
            }
            currentChunk.add(variantRuns);
        }
        return splitCollection;
    }
}
