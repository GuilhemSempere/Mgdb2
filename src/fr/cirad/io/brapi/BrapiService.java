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

import jhi.brapi.api.*;
import jhi.brapi.api.authentication.*;
import jhi.brapi.api.calls.*;
import jhi.brapi.api.genomemaps.*;
import jhi.brapi.api.germplasm.BrapiGermplasm;
import jhi.brapi.api.germplasm.BrapiGermplasmAttributes;
import jhi.brapi.api.markerprofiles.*;
import jhi.brapi.api.markers.BrapiMarker;
import jhi.brapi.api.samples.BrapiSample;
import jhi.brapi.api.search.BrapiSearchResult;
import jhi.brapi.api.studies.*;

import retrofit2.Call;
import retrofit2.http.*;

public interface BrapiService {

    public static final String BRAPI_FIELD_germplasmDbId = "germplasmDbId";
    public static final String BRAPI_FIELD_sampleDbId = "sampleDbId";
    public static final String BRAPI_FIELD_extGermplasmDbId = "extGermplasmDbId";
    public static final String BRAPI_FIELD_germplasmExternalReferenceId = "extRefId";
    public static final String BRAPI_FIELD_germplasmExternalReferenceSource = "extRefSrc";

    @GET(value = "calls")
    public Call<BrapiListResource<BrapiCall>> getCalls(@Query(value = "pageSize") String var1, @Query(value = "page") String var2);

    @FormUrlEncoded
    @POST(value = "token")
    public Call<BrapiSessionToken> getAuthToken(@Field(value = "grant_type") String var1, @Field(value = "username") String var2, @Field(value = "password") String var3, @Field(value = "client_id") String var4);

    @GET(value = "studies-search")
    public Call<BrapiListResource<BrapiStudies>> getStudies(@Query(value = "studyType") String var1, @Query(value = "pageSize") String var2, @Query(value = "page") String var3);

    @GET(value = "studies/{id}/germplasm")
    public Call<BrapiListResource<BrapiGermplasm>> getStudyGerplasmDetails(@Path(value = "id") String var1, @Query(value = "pageSize") String var2, @Query(value = "page") String var3);

    @GET(value = "maps")
    public Call<BrapiListResource<BrapiGenomeMap>> getMaps(@Query(value = "species") String var1, @Query(value = "pageSize") String var2, @Query(value = "page") String var3);

    @GET(value = "maps/{id}/positions")
    public Call<BrapiListResource<BrapiMarkerPosition>> getMapMarkerData(@Path(value = "id") String var1, @Query(value = "linkageGroupName") Collection<String> var2, @Query(value = "pageSize") String var3, @Query(value = "page") String var4);

    @GET("markers")	// this is wrong, only kept to remain compatible with https://ics.hutton.ac.uk/germinate-demo/cactuar-devel/brapi/v1
    public Call<BrapiListResource<BrapiMarker>> getMarkerDetailsHack(@Path(value = "id") String markerDbId);

    @GET("markers/{id}")
    public Call<BrapiBaseResource<BrapiMarker>> getMarkerDetails(@Path(value = "id") String markerDbId);

    @GET("markers-search")
    public Call<BrapiListResource<BrapiMarker>> getMarkerInfo(@Query("markerDbIds") Set<String> markerDbIds, @Query("name") Set<String> name, @Query("matchMethod") String matchMethod, @Query("include") String include, @Query("type") String type, @Query("pageSize") String pageSize, @Query("page") String page);

    @POST(value = "markers-search")
    public Call<BrapiListResource<BrapiMarker>> getMarkerInfo_byPost(@Body Map<String, Object> body);

    @GET(value = "markerprofiles")
    public Call<BrapiListResource<BrapiMarkerProfile>> getMarkerProfiles(@Query(value = "studyDbId") String var1, @Query(value = BRAPI_FIELD_germplasmDbId) Collection<String> var2, @Query(value = "pageSize") String var3, @Query(value = "page") String var4);

    @GET(value = "allelematrix-search")
    public Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix(@Query(value = "markerprofileDbId") List<String> var1, @Query(value = "markerDbId") List<String> var2, @Query(value = "format") String var3, @Query(value = "expandHomozygotes") Boolean var4, @Query(value = "unknownString") String var5, @Query(value = "sepPhased") String var6, @Query(value = "sepUnphased") String var7, @Query(value = "pageSize") String var8, @Query(value = "page") String var9);

    @POST(value = "allelematrix-search")
    public Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix_byPost(@Body Map<String, Object> var1);

    @GET(value = "allelematrix-search/status/{id}")
    public Call<BrapiBaseResource<Object>> getAlleleMatrixStatus(@Path(value = "id") String var1);

//    @GET(value="attributes")// v2.0
//    public Call<BrapiListResource<Object>> getAttributes(@Query(value=BRAPI_FIELD_germplasmDbId) String germplasmDbId);
    @GET(value = "germplasm/{germplasmDbId}/attributes")// v1.3
    public Call<BrapiBaseResource<BrapiGermplasmAttributes>> getAttributes(@Path(value = BRAPI_FIELD_germplasmDbId) String germplasmDbId);

    /* V1.3 calls */
    @POST(value = "search/germplasm")
    public Call<BrapiBaseResource<BrapiSearchResult>> searchGermplasm(@Body Map<String, Object> body);

    @POST(value = "search/germplasm")
    public Call<BrapiListResource<BrapiGermplasm>> searchGermplasmDirectResult(@Body Map<String, Object> body);

    @GET(value = "search/germplasm/{searchResultDbId}")
    public Call<BrapiListResource<BrapiGermplasm>> searchGermplasmResult(@Path(value = "searchResultDbId") String searchResultDbId, @Query(value = "pageSize") String var1, @Query(value = "page") String var2);

    @POST(value = "search/samples")
    public Call<BrapiBaseResource<BrapiSearchResult>> searchSamples(@Body Map<String, Object> body);

    @POST(value = "search/samples")
    public Call<BrapiListResource<BrapiSample>> searchSamplesDirectResult(@Body Map<String, Object> body);

    @GET(value = "search/samples/{searchResultDbId}")
    public Call<BrapiListResource<BrapiSample>> searchSamplesResult(@Path(value = "searchResultDbId") String searchResultDbId, @Query(value = "pageSize") String var1, @Query(value = "page") String var2);

}
