package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper

import groovy.transform.CompileStatic
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.DistanceUnit
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.GeoDistanceSortBuilder
import org.elasticsearch.search.sort.SortBuilder
import org.elasticsearch.search.sort.SortOrder

@CompileStatic
class ThingFinder {

    final String INDEX_NAME = 'things'
    final String DATA_TYPE = 'thing'

    final Client client
    final JsonHelper jsonHelper

    ThingFinder(Client client, ObjectMapper mapper) {
        this.client = client
        jsonHelper = new JsonHelper(mapper)
    }

    List<Thing> findThing(String query) {

        // Lower case the search term
        QueryBuilder builder =  QueryBuilders.termQuery('city', query.toLowerCase())
        println "  SEARCH QUERY >> \n${builder}"

        FilterBuilder filter = null
        SortBuilder sort = null
        return runQuery(10, builder, filter, sort)
    }

    List<Thing> locationQuery(Location location,
                              double distance = 500,
                              DistanceUnit units = DistanceUnit.MILES,
                              int maxCount = 10) {

        QueryBuilder query = QueryBuilders.matchAllQuery()


        FilterBuilder filter =  FilterBuilders.geoDistanceFilter('location')
                .distance(distance, units)
                .lat(location.lat)
                .lon(location.lon)

        SortBuilder sort = new GeoDistanceSortBuilder('location')
                .point(location.lat, location.lon)
                .unit(units)
                .order(SortOrder.ASC)

        return runQuery(maxCount, query, filter, sort)
    }

    private List<Thing> runQuery(int maxCount, QueryBuilder queryBuilder, FilterBuilder filterBuilder, SortBuilder sortBuilder ) {
        SearchRequestBuilder builder = client.prepareSearch(INDEX_NAME)
                .setTypes(DATA_TYPE)
                .setQuery(queryBuilder)
                .setExplain(true)
                .setSize(maxCount)

        if (filterBuilder) { builder.setPostFilter(filterBuilder) }
        if (sortBuilder) { builder.addSort(sortBuilder) }

        SearchResponse response = builder.execute().actionGet()

        SearchHits hitContainer = response.hits
        Long hitCount =  hitContainer.totalHits
        List<SearchHit> hits = hitContainer.hits as List

        List<String> responses = hits.collect{ SearchHit hit -> hit.sourceAsString() }

        return responses.collect{ String it -> jsonHelper.fromJson(it) }
    }

}
