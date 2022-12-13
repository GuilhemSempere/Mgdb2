/** *****************************************************************************
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
 ******************************************************************************
 */
package fr.cirad.io.brapi;

import java.util.*;

import org.brapi.v2.model.Service;

import org.brapi.v2.model.Service.MethodsEnum;
import org.brapi.v2.model.ContentTypes;

public class ServiceInfoUtils {

    public static final String GET = "GET";
    public static final String POST = "POST";
    public static final List<String> JSON = Arrays.asList("application/json", "json");
    public static final List<String> TSV = Arrays.asList("text/tsv", "tsv");

    private List<Service> services;

    public ServiceInfoUtils(List<Service> services) {
        this.services = services;
    }

    boolean ensureGenotypesCanBeImported() {	// validate the calls that MUST be present
        if (!hasCall("studies-search", ContentTypes.APPLICATION_JSON, MethodsEnum.GET)) {
            return false;
        }
        if (!hasCall("maps", ContentTypes.APPLICATION_JSON, MethodsEnum.GET)) {
            return false;
        }
        if (!hasCall("maps/{mapDbId}/positions", ContentTypes.APPLICATION_JSON, MethodsEnum.GET)) {
            return false;
        }
        if (!hasCall("markerprofiles", ContentTypes.APPLICATION_JSON, MethodsEnum.GET)) {
            return false;
        }
        if (!hasCall("allelematrix-search", ContentTypes.APPLICATION_JSON, MethodsEnum.POST) && !hasCall("allelematrix-search", ContentTypes.TEXT_TSV, MethodsEnum.POST)) {
            return false;
        }

        return true;
    }

    boolean ensureGermplasmInfoCanBeImported() {	// validate that at least one of the MUST calls is present
        if (hasCall("search/germplasm", ContentTypes.APPLICATION_JSON, MethodsEnum.POST)) {
            return true;
        }
        if (hasCall("germplasm/{germplasmDbId}/attributes", ContentTypes.APPLICATION_JSON, MethodsEnum.GET)) {
            return true;
        }

        return false;
    }

    boolean hasCallSearchAttributes() {
        return hasCall("search/attributevalues", ContentTypes.APPLICATION_JSON, MethodsEnum.POST);
    }

    boolean hasCallSearchGermplasm() {
        return hasCall("search/germplasm", ContentTypes.APPLICATION_JSON, MethodsEnum.POST);
    }

    boolean hasCallSearchSamples() {
        return hasCall("search/samples", ContentTypes.APPLICATION_JSON, MethodsEnum.POST);
    }

    public boolean hasCall(String signature, ContentTypes datatype, MethodsEnum method) {
        for (Service service : services) {
            String implementedCall = service.getService();
            if (implementedCall.startsWith("/")) {
                implementedCall = implementedCall.substring(1);
            }
            if (implementedCall.equals(signature) && service.getMethods().contains(method)
                    && service.getDataTypes().contains(datatype)) {
                return true;
            }
        }

        return false;
    }

    boolean hasToken() {
        return hasCall("token", ContentTypes.APPLICATION_JSON, MethodsEnum.GET);
    }

    boolean hasPostMarkersSearch() {
        return hasCall("markers-search", ContentTypes.APPLICATION_JSON, MethodsEnum.POST);
    }

    boolean hasGetMarkersSearch() {
        return hasCall("markers-search", ContentTypes.APPLICATION_JSON, MethodsEnum.GET);
    }

    boolean hasMarkersDetails() {
        return hasCall("markers/{markerDbId}", ContentTypes.APPLICATION_JSON, MethodsEnum.GET);
    }

//	boolean hasMapsMapDbId()
//	{
//		return hasCall("maps/id", JSON, GET);
//	}
    boolean hasAlleleMatrixSearchTSV() {
        return hasCall("allelematrix-search", ContentTypes.TEXT_TSV, MethodsEnum.POST);
    }
}
