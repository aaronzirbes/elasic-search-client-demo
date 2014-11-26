package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import com.spatial4j.core.context.SpatialContext
import com.spatial4j.core.shape.Shape
import com.spatial4j.core.shape.impl.RectangleImpl

import groovy.transform.CompileStatic

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.get.GetIndexResponse
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.MappingMetaData
import org.elasticsearch.common.collect.ImmutableOpenMap
import org.elasticsearch.common.geo.ShapeRelation
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits

import static org.elasticsearch.index.query.FilterBuilders.*
import static org.elasticsearch.index.query.QueryBuilders.*

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
        Thing thing = new Thing(
            key: 'A001',
            name: 'Aaron',
            city: 'Minneapolis',
            state: 'MN',
            location: new Location(44.9833, 93.2667)
        )

        boolean worked = createMapping()
        println "Created mapping?: ${worked}"

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

        return responses.collect{ String it -> fromJson(it) }
    }

    private boolean indexExists() {
        // Creating index if none
        GetIndexResponse indexResponse = client.admin()
                .indices()
                .prepareGetIndex()
                .execute()
                .actionGet()

        List<String> indices = indexResponse.indices as List
        println "found indices: ${indices}"
        return (indices.contains(INDEX_NAME))

    }

    private boolean createIndex() {
        CreateIndexResponse response = client.admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .execute()
                .actionGet()

        if (response.isAcknowledged()) {
            println 'index created'
        }
    }

    private String getMapping() {
        GetMappingsResponse response = client.admin().indices()
                .prepareGetMappings(INDEX_NAME)
                .setTypes(DATA_TYPE)
                .execute()
                .actionGet()

        if (response.mappings) {
            ImmutableOpenMap indexMappings = response.mappings.get(INDEX_NAME)
            if (indexMappings) {
                MappingMetaData mapping = indexMappings.get(DATA_TYPE)
                return mapping.source()
            }
        }
        return null
    }

    private boolean createMapping() {

        String expectedMapping = getJsonFromResource('thing_type')

        println 'Pushing mapping template:'
        println expectedMapping
        println ''

        println 'creating index if missing'
        if (!indexExists()) { createIndex() }


        boolean putMapping = true

        String existingMapping = getMapping()
        if (existingMapping) {
            if (sameJson(existingMapping, expectedMapping)) {
                putMapping = false
                println 'mapping already exists.'
            } else {
                println 'mapping in elasticsearch is different than expected'
                println "Found: ${existingMapping}"
            }
        }

        if (putMapping) {
            PutMappingResponse response = client.admin()
                                                .indices()
                                                .preparePutMapping(INDEX_NAME)
                                                .setIndices(INDEX_NAME)
                                                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                                                .setType(DATA_TYPE)
                                                .setSource(expectedMapping)
                                                .execute()
                                                .actionGet()

            if (response.isAcknowledged()) {
                println 'Mapping created!'
                return true
            } else {
                println 'Failed to create mapping.'
                return false
            }
        }
        return false
    }


    public void deleteMapping() {
        DeleteMappingResponse actionGet = client.admin().indices().deleteMapping(
            new DeleteMappingRequest(INDEX_NAME).types(DATA_TYPE)
        ).actionGet()
    }


    private String (String path) {
            this.class.getResourceAsStream(path).text
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
    private String locationQuery(Integer number) {
        //return geoShapeQuery('location', number, 'states')

        // Use filter ?
        // GeoShapeFilterBuilder.geoShapeFilter(String name, ShapeBuilder shape, ShapeRelation.DISJOINT)
        // .relation(ShapeRelation.DISJOINT)
    }

    private void deleteThing(String key) {
        DeleteResponse response = client.prepareDelete(INDEX_NAME, DATA_TYPE, key)
            .execute()
            .actionGet()
    }

    private String toJson(Thing thing) {
        return mapper.writeValueAsString(thing)
    }

    private Thing fromJson(String json) {
        return mapper.readValue(json, Thing)
    }


    private boolean sameJson(String json1, String json2) {
        JsonNode node1 = mapper.readValue(json1, JsonNode)
        JsonNode node2 = mapper.readValue(json2, JsonNode)

        return node1 == node2
    }

    private String getJsonFromResource(String resource) {
        this.class.getResourceAsStream("/${resource}.json").text
    }

}
