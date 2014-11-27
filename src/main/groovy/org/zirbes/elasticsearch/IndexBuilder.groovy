package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.get.GetIndexResponse
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.MappingMetaData
import org.elasticsearch.common.collect.ImmutableOpenMap

import groovy.transform.CompileStatic


@CompileStatic
class IndexBuilder {

    final ObjectMapper mapper
    final Client client
    final JsonHelper jsonHelper

    IndexBuilder(Client client, ObjectMapper mapper) {
        this.mapper = mapper
        this.client = client
        this.jsonHelper = new JsonHelper(mapper)
    }

    boolean createMapping(String index, String type) {

        String expectedMapping = jsonHelper.getJsonFromResource('thing_type')

        println 'Pushing mapping template:'
        println expectedMapping
        println ''

        println 'creating index if missing'
        if (!indexExists(index)) { createIndex(index) }


        boolean putMapping = true

        String existingMapping = getMapping(index, type)
        if (existingMapping) {
            if (jsonHelper.sameJson(existingMapping, expectedMapping)) {
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
                                                .preparePutMapping(index)
                                                .setIndices(index)
                                                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                                                .setType(type)
                                                .setSource(expectedMapping)
                                                .execute()
                                                .actionGet()

            if (response.acknowledged) {
                println 'Mapping created!'
                return true
            } else {
                println 'Failed to create mapping.'
                return false
            }
        }
        return false
    }

    boolean createIndex(String index) {
        CreateIndexResponse response = client.admin()
                .indices()
                .prepareCreate(index)
                .execute()
                .actionGet()

        if (response.acknowledged) {
            println 'index created'
        }
    }

    String getMapping(String index, String type) {
        GetMappingsResponse response = client.admin().indices()
                .prepareGetMappings(index)
                .setTypes(type)
                .execute()
                .actionGet()

        if (response.mappings) {
            ImmutableOpenMap indexMappings = response.mappings.get(index)
            if (indexMappings) {
                MappingMetaData mapping = indexMappings.get(type)
                return mapping.source()
            }
        }
        return null
    }

    boolean indexExists(String index) {
        // Creating index if none
        GetIndexResponse indexResponse = client.admin()
                .indices()
                .prepareGetIndex()
                .execute()
                .actionGet()

        List<String> indices = indexResponse.indices as List
        println "found indices: ${indices}"
        return (indices.contains(index))

    }


    void deleteMapping(String index, String type) {
        DeleteMappingResponse actionGet = client.admin().indices().deleteMapping(
            new DeleteMappingRequest(index).types(type)
        ).actionGet()
    }

}
