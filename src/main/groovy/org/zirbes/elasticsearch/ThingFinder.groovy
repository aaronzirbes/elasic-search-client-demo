package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper

import com.spatial4j.core.shape.Shape

import groovy.transform.CompileStatic

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.geo.ShapeRelation
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits

import static org.elasticsearch.index.query.FilterBuilders.*
import static org.elasticsearch.index.query.QueryBuilders.*

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

        // QueryBuilder builder = boolQuery().must(termQuery('city', query.toLowerCase()))

        // Lower case the search term
        QueryBuilder builder = termQuery('city', query.toLowerCase())

        println "  SEARCH QUERY >> \n${builder}"
        println ''

        SearchResponse response = client.prepareSearch(INDEX_NAME)
                                        .setTypes(DATA_TYPE)
                                        .setQuery(builder)
                                        .setExplain(true)
                                        .setSize(10)
                                        .execute()
                                        .actionGet()

        SearchHits hitContainer = response.hits
        println "total hits: ${hitContainer.totalHits}"

        List<SearchHit> hits = hitContainer.hits as List

        List<String> responses = hits.collect{ SearchHit hit -> hit.sourceAsString() }

        return responses.collect{ String it -> jsonHelper.fromJson(it) }
    }

    // Coordinates: Minnesota?
    String locationQuery(Integer number) {
        //return geoShapeQuery('location', number, 'states')

        // Use filter ?
        // GeoShapeFilterBuilder.geoShapeFilter(String name, ShapeBuilder shape, ShapeRelation.DISJOINT)
        // .relation(ShapeRelation.DISJOINT)
    }

}
