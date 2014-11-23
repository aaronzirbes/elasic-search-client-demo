package org.zirbes.elasticsearch

class DemoClient {

    static void main(String[] argv){

        println "Init."

        SearchClient searchClient = new SearchClient()
        searchClient.runTheThings()

        println "Done."

    }

}
