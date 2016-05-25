package org.casezero.di;

import org.casezero.di.datamonster.es.EsClient;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMonster {

	private static final Logger log = LoggerFactory.getLogger(DataMonster.class);
	
    /**
     * play with ES client
     * @param args
     */
    public static void main (String [] args){
        EsClient esClient = new EsClient();
        String alias = "food";
        String type = "yummy";
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        String index = esClient.generateIndexFromAlias(alias);
        esClient.setupIndex(index);
        IndexResponse response = esClient.putData(index, type, json);
        esClient.updateAlias(index, alias);

        log.info("SUCCESS Have client and inserted record");
        esClient.close();
    }
}
