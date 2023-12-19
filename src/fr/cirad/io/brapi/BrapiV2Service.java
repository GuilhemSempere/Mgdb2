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

import jhi.brapi.api.authentication.*;
import org.brapi.v2.model.GermplasmAttributeValueListResponse;
import org.brapi.v2.model.ServerInfoResponse;
import org.brapi.v2.model.SuccessfulSearchResponse;
import org.brapi.v2.model.GermplasmListResponse;
import org.brapi.v2.model.SampleListResponse;

import retrofit2.Call;
import retrofit2.http.*;

public interface BrapiV2Service {

    //get list of available calls in brapi V2
    @GET(value = "serverinfo")
    public Call<ServerInfoResponse> getServerInfo(@Query(value = "dataType") String var1);

    @FormUrlEncoded
    @POST(value = "token")
    public Call<BrapiSessionToken> getAuthToken(@Field(value = "grant_type") String var1, @Field(value = "username") String var2, @Field(value = "password") String var3, @Field(value = "client_id") String var4);

    @POST(value = "search/attributevalues")
    public Call<SuccessfulSearchResponse> searchAttributes(@Body Map<String, Object> body);

    @POST(value = "search/attributevalues")
    public Call<GermplasmAttributeValueListResponse> searchAttributesDirectResult(@Body Map<String, Object> body);

    @GET(value = "search/attributevalues/{searchResultsDbId}")
    public Call<GermplasmAttributeValueListResponse> searchAttributesResult(@Path(value = "searchResultsDbId") String searchResultDbId, @Query(value = "pageSize") String var1, @Query(value = "page") String var2);

    @POST(value = "search/germplasm")
    public Call<SuccessfulSearchResponse> searchGermplasm(@Body Map<String, Object> body);

    @POST(value = "search/germplasm")
    public Call<GermplasmListResponse> searchGermplasmDirectResult(@Body Map<String, Object> body);

    @GET(value = "search/germplasm/{searchResultsDbId}")
    public Call<GermplasmListResponse> searchGermplasmResult(@Path(value = "searchResultsDbId") String searchResultDbId, @Query(value = "pageSize") String var1, @Query(value = "page") String var2);

    @POST(value = "search/samples")
    public Call<SuccessfulSearchResponse> searchSamples(@Body Map<String, Object> body);

    @POST(value = "search/samples")
    public Call<SampleListResponse> searchSamplesDirectResult(@Body Map<String, Object> body);

    @GET(value = "search/samples/{searchResultsDbId}")
    public Call<SampleListResponse> searchSamplesResult(@Path(value = "searchResultsDbId") String searchResultDbId, @Query(value = "pageSize") String var1, @Query(value = "page") String var2);

}
