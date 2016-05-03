package org.casezero.di.datamonster.es;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.shield.ShieldPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by jlmelbourne on 5/2/16.
 */
public class EsClient {

    private static final Logger log = LoggerFactory.getLogger(EsClient.class);
    private static final String CLUSTER_ID = "8559709cdb41fc3e408d561d8f247623"; // Your cluster ID here
    private static final String REGION = "us-west-1"; // Your region here
    private static final boolean ENABLE_SSL = true;

    /**
     * setup the client
     */
    public Client getClient (){
        try {
            // setup ES client settings
            Settings settings = Settings.settingsBuilder()
                    .put("transport.ping_schedule", "5s")
                    .put("cluster.name", CLUSTER_ID)
                    .put("action.bulk.compress", false)
                    .put("shield.transport.ssl", ENABLE_SSL)
                    .put("request.headers.X-Found-Cluster", CLUSTER_ID)
                    .put("shield.user", "readwrite:veb3dp0a3dl") // your shield username and password
                    .build();

            // construct hostname
            String hostname = CLUSTER_ID + "." + REGION + ".aws.found.io";

            // Build client
            Client client = TransportClient.builder()
                    .addPlugin(ShieldPlugin.class)
                    .settings(settings)
                    .build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9343));
            log.info("Returning ES client ", hostname);
            return client;
        }
        catch (UnknownHostException e) {
            log.error("Unable to connect to client", e);
        }
        return null;
    }

    public static void main (String [] args){
        EsClient esClient = new EsClient();
        Client client = esClient.getClient();
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2014-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        IndexResponse response = client.prepareIndex("food", "yummy")
                .setSource(json)
                .get();

        log.info("SUCCESS Have client and inserted record");
        client.close();
    }



}
