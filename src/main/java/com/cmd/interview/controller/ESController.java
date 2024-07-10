package com.cmd.interview.controller;


import com.cmd.interview.model.Interview;
import com.cmd.interview.utils.ESUtils;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexResponse;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Map;
import java.util.HashMap;


/**
 * (ES)面试经验分享控制层
 */
@Slf4j
@RestController
@RequestMapping("/ES")
public class ESController {

    /**
     * @return
     */
    @GetMapping("/index")
    // @ApiOperation(value="创建索引-只创建索引", notes="创建索引-只创建索引")
    public Map<String, Object> index() {
        Map<String, Object> stringObjectMap = new HashMap<>();
        stringObjectMap.put("aaa", "bbb");
        return stringObjectMap;
    }

    /**
     * 创建索引
     *
     * @return
     */
    @GetMapping("/createIndex")
    public Map<String, Object> createIndex(@RequestParam("indexName") String indexName) throws IOException {
        if (!ESUtils.isIndexExist(indexName)) {
            ESUtils.createIndex(indexName);
        } else {
            // 索引已经存在
            Map<String, Object> code = new HashMap<>();
            code.put("code", "401");
            code.put("data", "索引已经存在");
            return code;
        }
        Map<String, Object> stringObjectMap = new HashMap<>();
        stringObjectMap.put("code", "200");
        stringObjectMap.put("data", "成功");
        return stringObjectMap;
    }

    /**
     * 将文件 文档信息储存到数据中
     *
     * @param
     * @return
     */
    @PostMapping("/insertFile")
    // @ApiOperation(value="创建索引ES-传入ES索引-传入文件", notes="创建索引ES-传入ES索引-传入文件")
    public Map<String, Object> insertFile(@RequestBody Interview i) throws IOException {
        // 创建要插入的文件
        Interview interview = new Interview();
        interview.setId(Integer.parseInt(String.valueOf(System.currentTimeMillis() / 1000)));
        interview.setUserId(i.getUserId());
        interview.setTitle(i.getTitle());
        interview.setCreateBy(i.getCreateBy());
        interview.setPosition(i.getPosition());
        interview.setCompany(i.getCompany());
        interview.setCreateTime(new Timestamp(System.currentTimeMillis()));

        // 文件转base64
        byte[] bytes = new byte[0];
        try {
            bytes = i.getContent().getBytes();
            //将文件内容转化为base64编码 这样es才能使用IK_smart识别
            String base64 = Base64.getEncoder().encodeToString(bytes);
            interview.setContent(base64);

            IndexResponse indexResponse = ESUtils.upload(interview);
            if (0 == indexResponse.status().getStatus()) {
                // 索引创建并插入数据成功
                System.out.println("索引创建并插入数据成功");
            }
            Map<String, Object> code = new HashMap<>();
            code.put("code", "200");
            code.put("data", "成功");
            return code;

        } catch (Exception e) {
            e.printStackTrace();
        }

        Map<String, Object> code = new HashMap<>();
        code.put("code", "402");
        code.put("data", "失败");
        return code;
    }

    /**
     * 文档内容检索
     *
     * @param content
     * @return
     */
    @GetMapping("/getFile")
    @ResponseBody
    public Map<String, Object> getFileData(@RequestParam(value = "content", required = true) String content) {
        log.info("content = {}", content);
        try {
            // 检索内容不能为空
            if (StringUtils.isEmpty(content)){
                Map<String, Object> code = new HashMap<>();
                code.put("code", "403");
                code.put("data", "检索内容不能为空");
                return code;
            }

            Map<String, Object> code = new HashMap<>();
            code.put("code", "200");
            code.put("data", ESUtils.search(content));
            return code;
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 系统异常
        Map<String, Object> code = new HashMap<>();
        code.put("code", "403");
        code.put("data", "系统异常");
        return code;
    }

    /**
     * 文档内容检索
     * @return
     */
    @GetMapping("/getFileAll")
    @ResponseBody
    public Map<String, Object> getFileDataAll() {
        try {

            Map<String, Object> code = new HashMap<>();
            code.put("code", "200");
            code.put("data", ESUtils.searchAll());
            return code;
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 系统异常
        Map<String, Object> code = new HashMap<>();
        code.put("code", "403");
        code.put("data", "系统异常");
        return code;
    }
}


