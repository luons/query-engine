package com.luons.engine.common;

import com.luons.engine.utils.CommonUtils;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class PageBean<T> implements Serializable {
    /**
     * 总页数
     */
    private Integer totalCount;
    /**
     * 第 n 页
     */
    private Integer pageIndex;
    /**
     * 每页条数
     */
    private Integer pageSize;

    private List<T> data;

    public PageBean() {
    }

    public PageBean(List<T> data) {
        this.data = data;
    }

    public PageBean(Integer pageIndex, Integer pageSize) {
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public PageBean(Integer pageIndex, Integer pageSize, Integer totalCount) {
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
        this.totalCount = totalCount;
    }

    public void setData(List<T> data) {
        if (data == null || data.isEmpty()) {
            this.data = new ArrayList<>();
            return;
        }
        this.data = data;
        if (pageIndex == null || pageSize == null) {
            return;
        }
        this.totalCount = pageSize;
        if (data.size() > pageSize) {
            this.totalCount = data.size();
            this.data = CommonUtils.list2Page(data, this.pageIndex, this.pageSize);
        }
    }

}
