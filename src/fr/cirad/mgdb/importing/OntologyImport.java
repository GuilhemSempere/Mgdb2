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
package fr.cirad.mgdb.importing;

import fr.cirad.tools.mongo.MongoTemplateManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * parse an .obo file to get ontology Id and name
 * supports both local or remote file (http / ftp )
 *
 * @author petel
 */
public class OntologyImport {

    private static final Logger LOG = LoggerFactory.getLogger(OntologyImport.class);

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new Exception("you have to provide at list a path to the .obo file");
        }
        Map<String, String> ontologyMap = new LinkedHashMap<>();
        String filePath = args[0];

        BufferedReader bufferedReader = null;
        String line;

        if (args[0].startsWith("http") | args[0].startsWith("ftp://")) {

            URL url = new URL(filePath);
            URLConnection connection = url.openConnection();
            InputStream input = connection.getInputStream();
            bufferedReader = new BufferedReader(new InputStreamReader(input));

        } else {
            File file = new File(filePath);
            bufferedReader = new BufferedReader(new FileReader(file));
        }

        int j = 0;
        // source version is the date in the header line beginning with "data-version"
        // line looks like : data-version: so-xp/releases/2015-11-24/so-xp.owl
        String date = "";
        String regex = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
        while ((line = bufferedReader.readLine()) != null && j < 5) {

            if (line.startsWith("data-version")) {

                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    date = matcher.group();
                }
            }
            j++;
        }
        ontologyMap.put("version", date);

        while ((line = bufferedReader.readLine()) != null) {
            // in .obo file, ontology are stored like this:
            // [Term]
            // id: translation_of
            // name: translation_of
            // ...
            if (line.startsWith("[Te")) {
                String id = bufferedReader.readLine().substring(4);
                String name = bufferedReader.readLine().substring(6);
                ontologyMap.put(name, id);
            }
        }
        bufferedReader.close();
        LOG.debug("downloaded ontology from " + args[0] + ", source version : " + date);
        MongoTemplateManager.setOntologyMap(ontologyMap);
    }

}
