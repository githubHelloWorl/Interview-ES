package com.cmd.interview.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Interview implements Serializable {

    /**
     * 文档 ID
     */
    private Integer id;

    /**
     * 用户 id
     */
    private Integer userId;

    /**
     * 标题
     */
    private String title;

    /**
     * 作者
     */
    private String createBy;

    /**
     * 面试职位
     */
    private String position;

    /**
     * 面试公司
     */
    private String company;

    /**
     * 内容
     */
    private String content;

    /**
     * 创建时间：
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss",timezone="GMT+8")
    private Timestamp createTime;
}