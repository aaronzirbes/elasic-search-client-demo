package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper

import groovy.transform.CompileStatic

import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client

@CompileStatic
class SearchClient {

    ObjectMapper mapper = new ObjectMapper()
    Client client

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

        // on shutdown
        client.close()
    }

    void showResults(IndexResponse response) {
        println " Index name ${response.index}"
        println " Type name ${response.type}"
        println " Document ID (generated or not) ${response.id}"
        println " Version (if it's the first time you index this document, you will get: 1) ${response.version}"
    }

    private String putThing(Thing thing) {

        String json = toJson(thing)

        IndexResponse response = client.prepareIndex('things', 'thing', thing.key)
                                       .setSource(json)
                                       .execute()
                                       .actionGet()

        showResults response

        return response.id
    }

    private Thing getThing(String key) {

        GetResponse response = client.prepareGet('things', 'thing', key)
                                     .execute()
                                     .actionGet()

        String json = response.sourceAsString

        Thing thing = mapper.readValue(json, Thing)

        println "Thing :: ${thing}"

        return thing
    }

    private void createIndex() {
        String indexData = getJsonFromResource('/resources/')
        // TODO
    }

    private String toJson(Object object) {
        return mapper.writeValueAsString(object)
    }

    private String getJsonFromResource(String path) {
        this.class.getResourceAsStream(path).text
    }

}
