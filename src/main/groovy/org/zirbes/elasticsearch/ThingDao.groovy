package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.ObjectMapper

import groovy.transform.CompileStatic

import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client

@CompileStatic
class ThingDao {

    static final String INDEX_NAME = 'things'
    static final String DATA_TYPE = 'thing'

    final Client client
    final JsonHelper jsonHelper

    ThingDao(Client client, ObjectMapper mapper) {
        this.client = client
        jsonHelper = new JsonHelper(mapper)
    }

    String putThing(Thing thing) {

        String json = jsonHelper.toJson(thing)

        IndexResponse response = client.prepareIndex(INDEX_NAME, DATA_TYPE, thing.key)
                                       .setSource(json)
                                       .execute()
                                       .actionGet()

        return response.id
    }

    Thing getThing(String key) {

        GetResponse response = client.prepareGet(INDEX_NAME, DATA_TYPE, key)
                                     .execute()
                                     .actionGet()

        if (response.exists) {
            String json = response.sourceAsString
            Thing thing = jsonHelper.fromJson(json)
            println "Thing :: ${thing}"
            return thing
        }
        return null
    }

    void deleteThing(String key) {
        DeleteResponse response = client.prepareDelete(INDEX_NAME, DATA_TYPE, key)
            .execute()
            .actionGet()
    }

}
