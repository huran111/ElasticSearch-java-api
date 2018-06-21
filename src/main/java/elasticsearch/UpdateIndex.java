package elasticsearch;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;

import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

public class UpdateIndex {
	private TransportClient client;

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
	public void search() {
		SearchResponse response = client.prepareSearch("huran").setTypes("huran")
				.setQuery(QueryBuilders.matchAllQuery()).setSize(10).execute().actionGet();
		System.out.println("getScrollId:" + response.getScrollId());
		System.out.println(response.getHits().getTotalHits());
		System.out.println(response.getHits().hits().length);
		List<Aggregation> asList = response.getAggregations().asList();

		for (SearchHit hit : response.getHits()) {
			String json = hit.getSourceAsString();
			System.out.println(json);
		}

	}

	/**
	 * 一次获取多个文档
	 */
	@Test
	public void updateAndInsert() {
		MultiGetResponse multiGetResponse = client.prepareMultiGet().add("huran", "huran", "AWQcIb51-SoXa-tT91rF")
				.add("huran", "huran", "AWQcIiR7-SoXa-tT91rG").add("fendo", "f endodate", "AWQcInlC-SoXa-tT91rH").get();
		for (MultiGetItemResponse itemResponse : multiGetResponse) {
			GetResponse response = itemResponse.getResponse();
			if (response.isExists())// 判断是否存在
			{
				String sourceAsString = response.getSourceAsString();
				System.out.println(sourceAsString);
			}
		}

	}

	/**
	 * 批量插入
	 * 
	 * @throws IOException
	 */
	@Test
	public void bulkInsert() throws IOException {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		bulkRequest.add(client.prepareIndex("a", "a", "1").setSource(XContentFactory.jsonBuilder().startObject()
				.field("user", "huran").field("postDate", new Date()).field("message", "sdfsdf").endObject()));
		bulkRequest.add(client.prepareIndex("a", "a", "2").setSource(XContentFactory.jsonBuilder().startObject()
				.field("sdf", "sdf").field("sdf", "dsf").field("sdf", "sdf").endObject()));
		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
			System.out.println("---------------");
		}
	}

	/**
	 * Bulk Processor 提供了一个简单的接口，在给定大小数量上定时批量自动请求
	 */
	@Test
	public void bickProcessor() {
		BulkProcessor.builder(client, new BulkProcessor.Listener() {

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				int numberOfActions = request.numberOfActions();
				System.out.println("numberOfActions:" + numberOfActions);

			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				String message = failure.getMessage();
				System.out.println("失败信息:" + message);
			}


			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				boolean hasFailures = response.hasFailures();
				System.out.println("失败数量:" + hasFailures);
			}
		}).setBulkActions(10000)// 每次请求10000
				.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)).setFlushInterval(TimeValue.timeValueSeconds(5))
				.setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
	}

	@Test
	public void search2() {
		client.prepareSearch("a", "huran").setTypes("a", "huran").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.termQuery("user", "a"))
				.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18)).setFrom(0).setSize(20).setExplain(true)
				.get();

	}

	/**
	 * 滚动搜索
	 */
	@Test
	public void scrollSearch() {

	}

}
