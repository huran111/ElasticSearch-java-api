package elasticsearch.QueryDSL;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import javax.management.Query;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author:HuRan
 * @Description:
 * @Date: Created in 10:42 2018/6/21
 * @Modified By:
 */
public class QueryTest {
    private TransportClient client;

    /**
     * 最简单的查询 查询所有文档
     */

    public void matchAllQuery() {
        QueryBuilder qb = QueryBuilders.matchAllQuery();
    }

    /**
     * 匹配查询
     */
    public void matchQuery() {
        QueryBuilder qb = QueryBuilders.matchQuery("fields", "value");
        //多字段查询
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("fields1", "value1"
                , "fields2", "value2");
        //常用术语查询 可以一些比较专业的偏门词语进行的更加专业的查询
        QueryBuilder queryBuilder1 = QueryBuilders.commonTermsQuery("name", "value");
        //查询语句查询  与Lucene查询语句的语法结合的更加紧密的一种查询 ，允许你在一个查询中使用多个特殊
        //条件关键字如：AND,OR，NOT对多个字段进行查询，当然这种查询仅限“专家用户”去用
        QueryBuilder queryBuilder2 = QueryBuilders.queryStringQuery("+kiemch -elasticsearch");
        //简单查询语句  是一种适合直接暴露给用户的且具有非常完善的查询语法的查询语句
        QueryBuilder queryBuilder3 = QueryBuilders.simpleQueryStringQuery("+kimchy	-elasticsearch ");                //text

    }

    /**
     * 虽然全文查询将在执行之前分析查询字符串，但是顶级级别查询对存储在反向索引中的确切项进行操作
     * 通常用于结构化数据，如数字，日期和枚举类，而不是全文字段，或者，在分析过程之前，它允许你绘制低级查询
     */
    public void termQuery() throws UnknownHostException {
        //查询包含在指定字段中指定的确切值的文档
        QueryBuilder qb = QueryBuilders.termQuery("fields", "value");
        //查询包含一个或者多个指定字段中指定的多个确切值的文档
        QueryBuilder queryBuilder = QueryBuilders.termsQuery("fields", "value1", "value2");
        //范围查询  查询指定字段包含指定范围内的值
        /**
         * 1.	gte()	:范围查询将匹配字段值大于或等于此参数值的文档。
         2.	gt():范围查询将匹配字段值大于此参数值的文档。
         3.	lte():范围查询将匹配字段值小于或等于此参数值的文档。
         4.	lt():范围查询将匹配字段值小于此参数值的文档。
         5.	from()开始值to()结束值这两个函数与includeLower()和includeUpper()函数配 套使用。
         6.	includeLower(true)表示from()查询将匹配字段值大于或等于此参数值的文 档。
         7.	includeLower(false)	表示from()查询将匹配字段值大于此参数值的文档。
         8.	includeUpper(true)表示to()查询将匹配字段值小于或等于此参数值的文档。
         9.	includeUpper(false)	表示to()查询将匹配字段值小于此参数值的文档
         */
        QueryBuilder queryBuilder1 = QueryBuilders.rangeQuery("fields")
                .from(5).to(10).includeLower(true).includeUpper(false);
        //存在查询
        ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery("name");
        Settings settings = Settings.builder().put("cluster.name", "my-application")
                .put("client.transport.sniff", true).build();

        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        client.prepareSearch("index")
                .setTypes("types")
                .setQuery(existsQueryBuilder).get();
        //前缀查询  查找指定的精确前缀开头的值的文档
        QueryBuilder queryBuilder2 = QueryBuilders.prefixQuery("field", "value");
        //通配符查询   查询指定字段包含指定模式匹配的值的文档，其中该模式支持单字符统配  ？ 和多字符统配 *
        // 而后者的匹配字符则受到限制。 和前缀查询一样，通配符查询指定字段是未分析的 （not	analyzed）
        QueryBuilder queryBuilder3 = QueryBuilders.wildcardQuery("user", "k?mc*");
        //正则表达式查询

    }
}
