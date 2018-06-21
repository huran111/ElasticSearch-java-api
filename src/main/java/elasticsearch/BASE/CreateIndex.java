package elasticsearch.BASE;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 创建索引
 * 
 * @author huran
 *
 */
public class CreateIndex {
	private TransportClient client;

	@SuppressWarnings("resource")
	@Before
	public void getClient() throws UnknownHostException {
		try {
			Settings settings = Settings.builder().put("cluster.name", "my-application")
					.put("client.transport.sniff", true).build();

			// 创建client
			client = new PreBuiltTransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (Exception e) {
			e.printStackTrace();
		}
		// 设置集群名称

	}

	/**
	 * 创建Json
	 */
	@Test
	public void CreateJson() {
		try {
			String json = "{" + "\"user\":\"fendo\"," + "\"postDate\":\"2013-01-30\"," + "\"message\":\"Hell	word\""
					+ "}";
			@SuppressWarnings("deprecation")
			IndexResponse response = client.prepareIndex("fendo", "fendodate").setSource(json).get();
			System.out.println("******" + response.getResult());
			// Index name
			String _index = response.getIndex();
			// Type name
			String _type = response.getType();
			// Document ID (generated or not)
			String _id = response.getId();
			// Version (if it's the first time you index this document, you will
			// get: 1)
			long _version = response.getVersion();
			System.out.println(_index + _id + _type);
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 使用集合
	 */
	@Test
	public void CreateList() {
		Map<String, Object> json = new HashMap<String, Object>();
		json.put("user", "kimchy");
		json.put("postDate", "2013-01-30");
		json.put("message", "trying	out	Elasticsearch");
		IndexResponse response = client.prepareIndex("fendo", "f endodate").setSource(json).get();
		System.out.println(response.getResult());
		System.out.println("id:" + response.getId());
		System.out.println("type:" + response.getType());
		System.out.println("index:" + response.getIndex());
	}
	@Test
	public  void aggregation(){
		SearchResponse searchResponse = client.prepareSearch("fendo").setTypes("f endodate")
		.setQuery(QueryBuilders.matchAllQuery())
		.addAggregation(AggregationBuilders.terms("user").field("field"))
		.addAggregation(AggregationBuilders.dateHistogram("postDate").field("birth")
				.dateHistogramInterval(DateHistogramInterval.YEAR)).get();
		Aggregation aggregation = searchResponse.getAggregations().get("user");
		Aggregation aggregation2 = searchResponse.getAggregations().get("postDate");
	}
	/**
	 * 使用ElasticSearch帮助类
	 * 
	 * @throws IOException
	 */
	@Test
	public void CreateXContentBuilder() throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("user", "ccase111")
				.field("postDate", new Date()).field("message", "this is elasticsearch").endObject();
		IndexResponse response = client.prepareIndex("huran", "huran").setSource(builder).get();

		System.out.println("创建成功" + response.getId());
	}

	/**
	 * operationThreaded 设置为 true 是在不同的线程里执行此次操作
	 */
	@Test
	public void get() {
		GetResponse response = client.prepareGet("huran", "huran", "AWQcAoqv-SoXa-tT91qy")
				.setOperationThreaded(true)
				.get();
		String sourceAsString = response.getSourceAsString();
		
		System.out.println(sourceAsString);
		GetResponse response2 = client.prepareGet("huran", "huran", "AWQcBklL-SoXa-tT91q0")
				.setOperationThreaded(true)
				.get();
		String sourceAsString2 = response2.getSourceAsString();
		System.out.println(sourceAsString2);
		
		
	}
	/**
	 * 删除
	 */
	@Test
	public void delete(){
		DeleteResponse response = client.prepareDelete("huran","huran","AWQb56rb-SoXa-tT91qs")
				.get();
		ShardInfo shardInfo = response.getShardInfo();
		System.out.println(shardInfo.getTotal());
		System.out.println(shardInfo.getFailed());
		System.out.println(shardInfo.getSuccessful());
		System.out.println(shardInfo.getFailures());
	}
	/**
	 * 通过查询条件删除
	 */
	@Test
	public void query(){
	/*	DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
		.filter(QueryBuilders.matchQuery("user", "ccase"))
		.source("huran")//索引名字
		.get();//执行
		
*/		
		try {
			//如果执行的时间比较长，可以使用异步的方式处理，结果在回调函数中获取
			DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
			.filter(QueryBuilders.matchQuery("user", "ccase111"))
			.source("huran").execute(new ActionListener<BulkByScrollResponse>() {
				
				@Override
				public void onResponse(BulkByScrollResponse response) {
					long deleted = response.getDeleted();//删除文档的数量
						System.out.println("删除文档数量:"+deleted);
				}
				
				@Override
				public void onFailure(Exception e) {
						e.printStackTrace();
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
}
