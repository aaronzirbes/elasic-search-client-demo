package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import groovy.transform.CompileStatic

@CompileStatic
class JsonHelper {

    final ObjectMapper mapper

    JsonHelper(ObjectMapper mapper) {
        this.mapper = mapper
    }

    String toJson(Thing thing) {
        return mapper.writeValueAsString(thing)
    }

    Thing fromJson(String json) {
        return mapper.readValue(json, Thing)
    }


    boolean sameJson(String json1, String json2) {
        JsonNode node1 = mapper.readValue(json1, JsonNode)
        JsonNode node2 = mapper.readValue(json2, JsonNode)

        return node1 == node2
    }

    String getJsonFromResource(String resource) {
        this.class.getResourceAsStream("/${resource}.json").text
    }

}
