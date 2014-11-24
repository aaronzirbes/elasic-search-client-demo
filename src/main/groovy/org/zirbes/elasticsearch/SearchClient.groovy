package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper

import static org.elasticsearch.index.query.QueryBuilders.*
import static org.elasticsearch.index.query.FilterBuilders.*

import groovy.transform.CompileStatic

import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.common.geo.ShapeRelation
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits


import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.shape.Shape
import com.spatial4j.core.shape.impl.RectangleImpl

@CompileStatic
class SearchClient {

    ObjectMapper mapper = new ObjectMapper()
    Client client

    final String INDEX_NAME = 'things'
    final String DATA_TYPE = 'thing'

    void runTheThings() {

        client = new ClientBuilder().withHostName('localhost')
                                    .withClusterName('elasticsearch_ajz')
                                    .buildTransportClient()
        println "Search."
        Thing thing = new Thing(
            key: 'A001',
            name: 'Aaron',
            city: 'Minneapolis',
            state: 'MN',
            location: new Location(44.9833, 93.2667)
        )

        String id = putThing(thing)
        println "Put thing: ${id}"

        Thing copyOfThing = getThing(thing.key)
        println "Got thing: ${copyOfThing.key}"

        List<Thing> foundThings = findThing('Minneapolis')
        println "Found things: ${foundThings}"

        deleteThing thing.key
        println "Deleted thing: ${thing.key}"

        Thing missingThing = getThing(thing.key)
        println "Got thing: ${missingThing?.key}"

        // on shutdown
        client.close()
    }

    private void showIndex(IndexResponse response) {
        println "    Index name ${response.index}"
        println "    Type name ${response.type}"
        println "    Document ID (generated or not) ${response.id}"
        println "    Version (if it's the first time you index this document, you will get: 1) ${response.version}"
    }

    private List<Thing> findThing(String query) {

        QueryBuilder builder = boolQuery().must(termQuery('city', query.toLowerCase()))
        // QueryBuilder builder = termQuery('city', query.toLowerCase())

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

        return responses.collect{ String it -> fromJson(it) }
    }


    private String putThing(Thing thing) {

        String json = toJson(thing)

        IndexResponse response = client.prepareIndex(INDEX_NAME, DATA_TYPE, thing.key)
                                       .setSource(json)
                                       .execute()
                                       .actionGet()

        showIndex response

        return response.id
    }

    private Thing getThing(String key) {

        GetResponse response = client.prepareGet(INDEX_NAME, DATA_TYPE, key)
                                     .execute()
                                     .actionGet()

        if (response.exists) {
            String json = response.sourceAsString
            Thing thing = fromJson(json)
            println "Thing :: ${thing}"
            return thing
        }
        return null
    }

    // Coordinates: Minnesota?
    private String locationQuery(Integer miles) {
        return geoShapeQuery('location', state, 'states')

        // Use filter ?
        // GeoShapeFilterBuilder.geoShapeFilter(String name, ShapeBuilder shape, ShapeRelation.DISJOINT)
        // .relation(ShapeRelation.DISJOINT)
    }

    private void deleteThing(String key) {
        DeleteResponse response = client.prepareDelete(INDEX_NAME, DATA_TYPE, key)
            .execute()
            .actionGet()
    }

    private void createIndex() {
        String indexData = getJsonFromResource('/resources/')
        // TODO
    }

    private String toJson(Thing thing) {
        return mapper.writeValueAsString(thing)
    }

    private Thing fromJson(String json) {
        return mapper.readValue(json, Thing)
    }

    private String getJsonFromResource(String path) {
        this.class.getResourceAsStream(path).text
    }

}
