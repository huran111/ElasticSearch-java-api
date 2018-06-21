package elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

public class ScrollsAPI  {
	private TransportClient client;
	private String scrollId;

	@SuppressWarnings("resource")
	@Before
	public void getClient() throws UnknownHostException {
		try {
			Settings settings = Settings.builder().put("cluster.name", "my-application")
					.put("client.transport.sniff", true).build();

			// 创建client
			client = new PreBuiltTransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.67.51.67"), 9300));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testScrolls(){
		SearchResponse searchResponse = client.prepareSearch("a","a")
		.addSort(FieldSortBuilder.DOC_FIELD_NAME,SortOrder.ASC)
		.setScroll(new TimeValue(60000))
		.setQuery(QueryBuilders.termQuery("user", "a"))
		.setSize(5).get();
		 scrollId = searchResponse.getScrollId();
		System.out.println("滚动ID:"+scrollId);
		do{
			for(SearchHit hit:searchResponse.getHits().getHits()){
				System.out.println("-----:"+hit.getSource().toString());
			}
			searchResponse = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(60000)).execute().actionGet();
		}while(searchResponse.getHits().getHits().length!=0);
	}
	public void tearDown(){
		ClearScrollRequestBuilder clearScrollRequestBuilder=client.prepareClearScroll();
		clearScrollRequestBuilder.addScrollId(scrollId);
		ClearScrollResponse clearScrollResponse = clearScrollRequestBuilder.get();
		if(clearScrollResponse.isSucceeded()){
				System.out.println("清除成功");
		}
		
	}
	@Test
    public void testMultiSearch() throws Exception {

        SearchRequestBuilder srb1 = client
                .prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);

        SearchRequestBuilder srb2 = client
                .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;
        for (MultiSearchResponse.Item item : sr.getResponses()) {
            SearchResponse response = item.getResponse();

            nbHits += response.getHits().getTotalHits();

        }

        System.out.println("nbHits:" + nbHits);

    }

}
