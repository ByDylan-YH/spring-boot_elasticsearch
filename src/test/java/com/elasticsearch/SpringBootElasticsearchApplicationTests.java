package com.elasticsearch;


import com.elasticsearch.entity.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
class SpringBootElasticsearchApplicationTests {
    private static ObjectMapper mapper = new ObjectMapper();
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    //    创建索引
    @Test
    void testCreateIndex() {
        CreateIndexRequest request = new CreateIndexRequest("bydylan");
        try {
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            log.info("Created index: {}", response.index());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    判断索引是否存在
    @Test
    void testExistIndex() {
        GetIndexRequest request = new GetIndexRequest("bydylan");
        try {
            log.info("index is exist: {}", client.indices().exists(request, RequestOptions.DEFAULT));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    删除索引
    @Test
    void testDeleteIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest("bydylan");
        try {
            AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
            log.info("index is deleted: {}", delete.isAcknowledged());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    添加文档
    @Test
    void testAddDocument() {
        User user = new User("BY", 25);
        // 创建请求
//        规则 put /bydylan/_doc/1
        IndexRequest request = new IndexRequest("bydylan");
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
//        或者
//        request.timeout("1s");
//        将数据放入请求 json
        IndexResponse indexResponse;
        try {
            request.source(mapper.writeValueAsString(user), XContentType.JSON);
//        客户端发送请求 , 获取响应的结果
            indexResponse = client.index(request, RequestOptions.DEFAULT);
            log.info("indexResponse: {}", indexResponse.toString());
            log.info("add status: {}", indexResponse.status());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    获取文档，判断是否存在 get /index/doc/1
    @Test
    void testIsExists() {
        GetRequest getRequest = new GetRequest("bydylan", "1");
//        不获取返回的 _source 的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        try {
            log.info("is exists: {}", client.exists(getRequest, RequestOptions.DEFAULT));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //    获得文档的信息
    @Test
    void testGetDocument() {
        GetRequest getRequest = new GetRequest("bydylan", "1");
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("打印文档的内容: {}", getResponse.getSourceAsString());
        log.info("返回的全部内容和命令式一样的: {}", getResponse);
    }

    //    更新文档的信息
    @Test
    void testUpdateRequest() {
        UpdateRequest updateRequest = new UpdateRequest("bydylan", "1");
//        立即生效
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        updateRequest.timeout("1s");
        User user = new User("BY", 18);
        UpdateResponse updateResponse = null;
        try {
            updateRequest.doc(mapper.writeValueAsString(user), XContentType.JSON);
            updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("update status: {}", updateResponse.status());
    }

    // 删除文档记录
    @Test
    void testDeleteRequest() {
        DeleteRequest request = new DeleteRequest("bydylan", "1");
        request.timeout("1s");
        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("delete status: {}", deleteResponse.status());
    }

    //    批处理请求,项目一般都会批量插入数据!!!
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("kuangshen1", 3));
        userList.add(new User("kuangshen2", 3));
        userList.add(new User("kuangshen3", 3));
        userList.add(new User("qinjiang1", 3));
        userList.add(new User("qinjiang1", 3));
        userList.add(new User("qinjiang1", 3));
        for (int i = 0; i < userList.size(); i++) {
//            批量更新和批量删除，就在这里修改对应的请求就可以了
            bulkRequest.add(
                    new IndexRequest("bydylan")
                            .id("" + (i + 1))
                            .source(mapper.writeValueAsString(userList.get(i)), XContentType.JSON));
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        boolean hasFailures = bulkResponse.hasFailures();
        log.info("bulk insert is fail: {},FailureMessage: ", hasFailures, hasFailures ? bulkResponse.buildFailureMessage() : null);
    }

    /**
     * 查询
     * SearchRequest 搜索请求
     * SearchSourceBuilder 条件构造
     * HighlightBuilder 构建高亮
     * TermQueryBuilder 精确查询
     * MatchAllQueryBuilder
     * xxx QueryBuilder 对应我们刚才看到的命令！
     */
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("bydylan");
//        构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        /*
        //        设置高亮
        HighlightBuilder highlighter = sourceBuilder.highlighter();
        highlighter.field("字段名");
//        查到的结果全部高亮吗?
        highlighter.requireFieldMatch(false);
        highlighter.preTags("<span style='color:red'>");
        highlighter.postTags("</span>");
        sourceBuilder.highlighter(highlighter);
         */
//        精确查找
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "qinjiang1");
//        匹配所有
        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        log.info("SearchHits: {}", mapper.writeValueAsString(searchResponse.getHits()));
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            /*
            //            解析高亮的字段
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            if (title != null) {
                StringBuilder newTitle = null;
                for (Text text : title.fragments()) {
                    newTitle.append(text);
                }
//                高亮的字段替换原来的字段
                sourceAsMap.put("title", newTitle.toString());
            }
            */
            log.info("documentFields: {}", documentFields.getSourceAsMap());
        }
    }
}
