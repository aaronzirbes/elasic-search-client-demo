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

        Location minneapolis = new Location(44.9833, 93.2667)
        Location humboltPark = new Location(41.902667, -87.702201)
        Location wahpeton = new Location(46.275492, -96.589358)

        boolean worked = indexBuilder.createMapping(INDEX_NAME, DATA_TYPE)
        println "Created mapping?: ${worked}"

        List<Thing> things = DataLoader.loadThings()

        /*
        things.each{ Thing thing ->
            String id = thingDao.putThing(thing)
            println "Put thing [${id}]: ${thing}"
        }

        things.each{ Thing thing ->
            Thing copyOfThing = thingDao.getThing(thing.key)
            println "Got thing: ${copyOfThing.key}"
        }
        */

        List<Thing> foundThings = thingFinder.findThing('Minneapolis')
        println 'Found things:'
        foundThings.each{ Thing thing ->
            println " * ${thing}"
        }

        List<Thing> thingsNear = thingFinder.locationQuery(minneapolis, 300)
        println 'Found close things:'
        thingsNear.each{ Thing thing ->
            println " * ${thing}"
        }


        /*
        things.each{ Thing thing ->
            thingDao.deleteThing thing.key
            println "Deleted thing: ${thing.key}"
        }

        things.each{ Thing thing ->
            Thing missingThing = thingDao.getThing(thing.key)
            println "Can we find thing: ${missingThing?.key}"
        }
        */

        println 'done.'

        // on shutdown
        client.close()
    }

}
