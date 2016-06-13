package org.casezero.di.datamonster.es;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.shield.ShieldPlugin;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Class to run ES transport Client
 * @author Jason Melbourne
 */
public class EsClient {

    private static final Logger log = LoggerFactory.getLogger(EsClient.class);

    private Client client;
    private String esUser;
    private String esPassword;
    private String esClusterID;
    private String esRegion;
    private boolean enableSSL;
    
    private BulkProcessor bulkProcessor;

    public EsClient() {
        setupClient();
    }

    /**
     * setup the ES transport client
     */
    private void setupClient (){

        // Get cluster properties
        Properties prop = new Properties();
        InputStream input = null;

        try {

            input = this.getClass().getResourceAsStream("/es.properties");

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            esUser = prop.getProperty("shield_user");
            esPassword = prop.getProperty("shield_password"); // TODO: encrypt
            esClusterID = prop.getProperty("es_cluster_id");
            esRegion = prop.getProperty("es_cluster_region");
            enableSSL = Boolean.parseBoolean(prop.getProperty("es_enable_ssl"));

        } catch (IOException e) {
            log.error("Error reading properties file", e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    log.error("Error, could not close input stream", e);
                }
            }
        }

        try {
            // setup ES client settings
            Settings settings = Settings.settingsBuilder()
                    .put("transport.ping_schedule", "5s")
                    .put("cluster.name", esClusterID)
                    .put("action.bulk.compress", false) //NEVER set this to true it hangs the processor
                    .put("shield.transport.ssl", enableSSL)
                    .put("request.headers.X-Found-Cluster", esClusterID)
                    .put("shield.user", new StringBuilder()
                            .append(esUser)
                            .append(":")
                            .append(esPassword)
                            .toString()) // shield username and password
                    .build();

            // construct hostname
            String hostname = new StringBuilder()
                    .append(esClusterID)
                    .append(".")
                    .append(esRegion)
                    .append(".aws.found.io")
                    .toString();

            // Build client
            client = TransportClient.builder()
                    .addPlugin(ShieldPlugin.class)
                    .settings(settings)
                    .build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9343));
            log.info("Returning ES client ", hostname);
        }
        catch (UnknownHostException e) {
            log.error("Unable to connect to client", e);
        }
        
        // set up bulk processor
        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                log.info("Executed bulk composed of {} actions", request.numberOfActions());
            }

            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.warn("Error executing bulk", failure);
            }
            }).build();
    }
    
    public void bulkFlush() {
    	bulkProcessor.flush();
    }

    public void deleteOldIndexes (String prefix) {
        ImmutableOpenMap<String, IndexMetaData> indexMap = client.admin().cluster()
                .prepareState().execute()
                .actionGet().getState()
                .getMetaData()
                .indices();
    }

    /**
     * setup ES index ahead of time, necessary if you want to do a mapping file
     * @param index
     */
    public void setupIndex(String index){
    	if (!(client.admin().indices().prepareExists(index).execute().actionGet().isExists()))
            client.admin().indices().create(new CreateIndexRequest(index)).actionGet();
    }
    
    public boolean hasIndex(String index) {
    	return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    }

    /**
     * Add mapping file to ES index
     * @param index
     * @param type
     * @param json
     * @return
     */
    public PutMappingResponse setupMapping(String index, String type, String json) {
        return client.admin().indices()
                .preparePutMapping(index)
                .setType(type)
                .setSource(json)
                .execute().actionGet();
    }

    /**
     * Add mapping file to ES index
     * @param index
     * @param type
     * @param json
     * @return
     */
    public PutMappingResponse setupMapping(String index, String type, XContentBuilder json) {
        return client.admin().indices()
                .preparePutMapping(index)
                .setType(type)
                .setSource(json)
                .execute().actionGet();
    }

    /**
     * Add data to ES Index record by record
     * @param index
     * @param type
     * @param json
     * @return
     */
    public void bulkAdd(String index, String type, String json) {
    	bulkProcessor.add(client.prepareIndex(index, type).setSource(json).request());
    }
    
    /**
     * Add data to ES Index record by record
     * @param index
     * @param type
     * @param json
     * @return
     */
    public IndexResponse putData(String index, String type, String json) {
        return client.prepareIndex(index, type)
                .setSource(json)
                .get();
    }

    /**
     * Add data to ES Index record by record
     * @param index
     * @param type
     * @param json
     * @return
     */
    public IndexResponse putData(String index, String type, byte[] json) {
        return client.prepareIndex(index, type)
                .setSource(json)
                .get();
    }

    /**
     * method to update an alias to an index with the transport client
     * @param newEsIndex
     * @param esAlias
     */
    public void updateAlias(String newEsIndex, String esAlias) {
        Set<String> oldEsIndexes = getIndicesFromAlias(esAlias);
        if (oldEsIndexes.isEmpty()) {
            addAlias(newEsIndex, esAlias);
        }
        else {
            client.admin().indices()
                    .prepareAliases()
                    .removeAlias((String []) oldEsIndexes.toArray(), esAlias)
                    .addAlias(newEsIndex, esAlias)
                    .execute()
                    .actionGet();
        }
    }

    /**
     * method to return all indicies that have a given alias
     * @param aliasName
     * @return
     */
    public Set<String> getIndicesFromAlias(String aliasName) {

        IndicesAdminClient iac = client.admin().indices();
        ImmutableOpenMap<String, List<AliasMetaData>> map = iac.getAliases(new GetAliasesRequest(aliasName))
                .actionGet().getAliases();

        final Set<String> allIndices = new HashSet<String>();
        while (map.keysIt().hasNext()) {
            allIndices.add(map.keysIt().next());
        }
        return allIndices;
    }

    /**
     * method to add an alias to an index with the transport client
     * @param esIndex
     * @param esAlias
     */
    public void addAlias(String esIndex, String esAlias) {
        client.admin().indices()
                .prepareAliases()
                .addAlias(esIndex, esAlias)
                .execute()
                .actionGet();
    }

    /**
     * close the ES transport client
     * @throws InterruptedException 
     */
    public void close() throws InterruptedException{
    	bulkProcessor.awaitClose(5, TimeUnit.MINUTES);
        client.close();
    }

    /**
     * method to generate a dated index from an alias
     * @param alias
     * @return
     */
    public static String generateIndexFromAlias (String alias){
        DateTime dateTime = DateTime.now();
        DateTimeFormatter dtfOut = DateTimeFormat.forPattern("yyyyMMdd");
        return new StringBuilder()
                .append(alias)
                .append("_")
                .append(dtfOut.print(dateTime))
                .toString();

    }
    
    /**
     * Delete index
     * @param index
     * 
     */
    public void deleteIndex(String index) {
    	DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    	if (!delete.isAcknowledged()) {
    		log.error("Index ("+ index +") was not deleted successfully");
    		// TODO: should app die here?
    	}
    }

    /**
     * return the client if needed
     * @return
     */
    public Client getClient() {
        return client;
    }
    
    public void reindex(String oldIndex, String newIndex) throws InterruptedException {
    	SearchResponse scrollResp = client.prepareSearch(oldIndex) // Specify index
    			.setSearchType(SearchType.DEFAULT)
    		    .setScroll(new TimeValue(60000))
    		    .setQuery(QueryBuilders.matchAllQuery()) // Match all query
    		    .setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll
    	
    	//Scroll until no hits are returned
    	while (true) {
    	    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
    	    //Break condition: No hits are returned
    	    if (scrollResp.getHits().getHits().length == 0) {
    	        bulkProcessor.awaitClose(5, TimeUnit.MINUTES);
    	        break; 
    	    }
    	    // Get results from a scan search and add it to bulk ingest
    	    for (SearchHit hit: scrollResp.getHits()) {
    	        IndexRequest request = new IndexRequest(newIndex, hit.type(), hit.id());
    	        Map source = ((Map) ((Map) hit.getSource()));
    	        request.source(source);
    	        bulkProcessor.add(request);
    	   }
    	}
    	
    	
    }
}
