package org.zirbes.elasticsearch

import groovy.transform.CompileStatic

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import static org.elasticsearch.node.NodeBuilder.nodeBuilder

@CompileStatic
class ClientBuilder {

    /*
     * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/
     * http://www.elasticsearch.org/guide/en/elasticsearch/client/groovy-api/current/
     */

    /*
     * Using Node client rather than Transport
     * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_transportclient_vs_nodeclient.html
     */

    private final String CLUSTER_NAME_SETTING = 'cluster.name'

    // Set to true to ignore cluster name validation of connected nodes. (since 0.19.4)
    private ignoreClusterName = false

    private String clusterName = 'elasticsearch'
    private String hostName = 'localhost'
    private int port = 9300

    ClientBuilder withClusterName(String clusterName) {
        this.clusterName = clusterName
        return this
    }

    ClientBuilder withHostName(String hostName) {
        this.hostName = hostName
        return this
    }

    ClientBuilder withPortNumber(int port) {
        this.port = port
        return this
    }

    ClientBuilder ignoreClusterName() {
        this.ignoreClusterName = true
        return this
    }

    Client buildTransportClient() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
        builder.put(CLUSTER_NAME_SETTING, clusterName)
        if (ignoreClusterName) {
            builder.put('client.transport.ignore_cluster_name' , 'true')
        }

        // The time to wait for a ping response from a node. Defaults to 5s.
        //client.transport.ping_timeout
        // How often to sample / ping the nodes listed and connected. Defaults to 5s.
        //client.transport.nodes_sampler_interval

        Settings settings = builder.build()
        TransportClient transportClient = new TransportClient(settings)
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(hostName, port))
        return transportClient
    }

}
