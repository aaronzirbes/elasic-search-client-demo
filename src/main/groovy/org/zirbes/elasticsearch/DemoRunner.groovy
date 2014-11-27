package org.zirbes.elasticsearch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import groovy.transform.CompileStatic

import org.elasticsearch.client.Client

@CompileStatic
class DemoRunner {

    final ObjectMapper mapper
    final Client client
    final IndexBuilder indexBuilder
    final JsonHelper jsonHelper
    final ThingFinder thingFinder
    final ThingDao thingDao

    final String INDEX_NAME = 'things'
    final String DATA_TYPE = 'thing'

    DemoRunner() {

        client = new ClientBuilder().withHostName('localhost')
                                    .withClusterName('elasticsearch_ajz')
                                    .buildTransportClient()
        mapper = new ObjectMapper()

        indexBuilder = new IndexBuilder(client, mapper)
        jsonHelper = new JsonHelper(mapper)
        thingDao = new ThingDao(client, mapper)
        thingFinder = new ThingFinder(client, mapper)

    }

    void run() {

        Thing thing = new Thing(
            key: 'A001',
            name: 'Aaron',
            city: 'Minneapolis',
            state: 'MN',
            location: new Location(44.9833, 93.2667)
        )

        boolean worked = indexBuilder.createMapping(INDEX_NAME, DATA_TYPE)
        println "Created mapping?: ${worked}"

        String id = thingDao.putThing(thing)
        println "Put thing: ${id}"

        Thing copyOfThing = thingDao.getThing(thing.key)
        println "Got thing: ${copyOfThing.key}"

        List<Thing> foundThings = thingFinder.findThing('Minneapolis')
        println "Found things: ${foundThings}"

        thingDao.deleteThing thing.key
        println "Deleted thing: ${thing.key}"

        Thing missingThing = thingDao.getThing(thing.key)
        println "Got thing: ${missingThing?.key}"

        // on shutdown
        client.close()
    }

}
