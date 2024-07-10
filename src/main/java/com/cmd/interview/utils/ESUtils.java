package com.cmd.interview.utils;


import com.alibaba.fastjson.JSON;
import com.cmd.interview.model.Interview;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class ESUtils {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private static RestHighLevelClient levelClient;

    @PostConstruct
    public void initClient() {
        levelClient = this.restHighLevelClient;
    }
    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) throws IOException {
        if (!isIndexExist(index)) {
            log.info("Index is not exists!");
        }

        CreateIndexRequest request = new CreateIndexRequest(index);
        // 添加 IK 分词器设置   ik分词器一定要存在 不然检索会报错
        //    request.settings(Settings.builder()
        //            .put("index.analysis.analyzer.default.type", "ik_max_word")
        //            .put("index.analysis.analyzer.default.use_smart", "true")
        //    );

        // 添加 IK 分词器设置 ik_smart 分词会更细一点
        request.settings(Settings.builder()
                .put("index.analysis.analyzer.default.type", "ik_smart")
        );
        CreateIndexResponse response = levelClient.indices().create(request, RequestOptions.DEFAULT);
        log.info("执行建立成功？" + response.isAcknowledged());
        return response.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        boolean exists = levelClient.indices().exists(request, RequestOptions.DEFAULT);
        if (exists) {
            log.info("Index [" + index + "] exists!");
        } else {
            log.info("Index [" + index + "] does not exist!");
        }
        return exists;
    }

    /**
     * 创建索引并插入数据
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static IndexResponse upload(Interview file) throws IOException {
        String indexName = "file1";
        // TODO 创建前需要判断当前文档是否已经存在
        if (!isIndexExist(indexName)) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            // 添加 IK 分词器设置  ik_max_word
            //    request.settings(Settings.builder()
            //            .put("index.analysis.analyzer.default.type", "ik_max_word")
            //            .put("index.analysis.analyzer.default.use_smart", "true")
            //    );

            // 添加 IK 分词器设置 ik_smart
            request.settings(Settings.builder()
                    .put("index.analysis.analyzer.default.type", "ik_smart")
            );
            CreateIndexResponse response = levelClient.indices().create(request, RequestOptions.DEFAULT);
            log.info("执行建立成功？" + response.isAcknowledged());
        }

        IndexRequest indexRequest = new IndexRequest(indexName);
        //上传同时，使用attachment pipline进行提取文件
        indexRequest.source(JSON.toJSONString(file), XContentType.JSON);
        indexRequest.setPipeline("attachment");
        IndexResponse indexResponse = levelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
        return indexResponse;
    }

    /**
     * 通过ID删除数据
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    public static void deleteDataById(String index, String type, String id) throws IOException {
        DeleteRequest request = new DeleteRequest(index, id);
        DeleteResponse response = levelClient.delete(request, RequestOptions.DEFAULT);

        // 处理响应
        DocWriteResponse.Result result = response.getResult();
        if(result == DocWriteResponse.Result.DELETED){
            System.out.println("文档已被成功删除");
        } else {
            System.out.println("文档不存在或已被删除");
        }
        log.info("deleteDataById response status:{}, id:{}", response.status().getStatus(), response.getId());
    }

    /**
     * 根据关键词，搜索对应的文件信息
     * 查询文件中的文本内容
     *
     * @param keyword
     * @throws IOException
     */
    public static List<Map<String, Object>> search(String keyword) throws IOException {
        String indexName = "file1";

        SearchRequest searchRequest = new SearchRequest(indexName);
        System.out.println(keyword + " : " + indexName);

        //默认会search出所有的东西来
        //SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchSourceBuilder srb = new SearchSourceBuilder();
        //多条件查询？
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // TODO
        boolQuery.should(QueryBuilders.matchQuery("attachment.content", keyword).analyzer("ik_smart"));
//                .should(QueryBuilders.matchQuery("name", keyword).analyzer("ik_smart"));
        // boolQuery.must(QueryBuilders.matchQuery("name", keyword))
        //         //使用lk分词器查询，会把插入的字段分词，然后进行处理
        //         .should(QueryBuilders.matchQuery("attachment.content", keyword).analyzer("ik_smart"));
        srb.query(boolQuery);

        //设置highlighting
        HighlightBuilder highlightBuilder = new HighlightBuilder();

        highlightBuilder.field("attachment.content");
        highlightBuilder.preTags("<span style='color: red;font-weight: bold;'>");
        highlightBuilder.postTags("</span>");

        //highlighting会自动返回匹配到的文本，所以就不需要再次返回文本了
        String[] includeFields = new String[]{"id", "name", "attachment.content", "fileSize", "filePath"};
        String[] excludeFields = new String[]{};
        // TODO
//        srb.fetchSource(includeFields, excludeFields);

        //把刚才设置的值导入进去
        srb.highlighter(highlightBuilder);

        // 设置返回的文档数量
        srb.size(1000);
        srb.from(0);

        searchRequest.source(srb);
        SearchResponse res = levelClient.search(searchRequest, RequestOptions.DEFAULT);

        //获取hits，这样就可以获取查询到的记录了
        SearchHits hits = res.getHits();

        //hits是一个迭代器，所以需要迭代返回每一个hits
        Iterator<SearchHit> iterator = hits.iterator();
        int count = 0;
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : res.getHits().getHits()) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();//原来的结果

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            for (String fieldName : highlightFields.keySet()) {
                HighlightField highlightField = highlightFields.get(fieldName);

                HighlightField title = highlightFields.get(highlightField.getName());
                sourceAsMap.put(highlightField.getName(), highlightField.getFragments()[0].string());
                ++count;
            }

            // 获取 _id
//            sourceAsMap.put("_id", hit.getId());
            sourceList.add(sourceAsMap);
            System.out.println(sourceAsMap);
        }
        System.out.println("查询到" + count + "条记录");
        // TODO 可以分页展示 new EsPage(startPage, pageSize, (int) totalHits, sourceList)
        return sourceList;
    }

    /**
     * 根据关键词，搜索对应的文件信息
     * 查询文件中的文本内容
     *
     * @throws IOException
     */
    public static List<Map<String, Object>> searchAll() throws IOException {
        String indexName = "file1";

        SearchRequest searchRequest = new SearchRequest(indexName); // 替换为你的索引名
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); // 使用match_all查询所有文档

        // 设置返回文档数量
        searchSourceBuilder.from(0).size(1000);

        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = levelClient.search(searchRequest, RequestOptions.DEFAULT);

            // 得到索引结果
            List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
            int i = 0;
            // 处理搜索结果
            for (SearchHit hit : searchResponse.getHits().getHits()) {

                Map<String, Object> sourceAsMap = hit.getSourceAsMap();//原来的结果
                ++i;

                // 获取 _id
//                sourceAsMap.put("_id", hit.getId());
                sourceList.add(sourceAsMap);
//                System.out.println(sourceAsMap);
            }
            System.out.println("数量为:");
            System.out.println(i);

            // 返回结果
            return sourceList;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
