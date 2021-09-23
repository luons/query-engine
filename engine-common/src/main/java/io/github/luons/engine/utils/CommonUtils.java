package io.github.luons.engine.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CommonUtils {

    public static <T> List<T> list2Page(List<T> list, Integer pageIndex, Integer pageSize) {
        if (list == null || list.isEmpty()) {
            return new ArrayList<>();
        }
        pageIndex = pageIndex == null ? 1 : pageIndex;
        pageSize = pageSize == null ? 10 : pageSize;
        int size = list.size();
        int pageCount = (size % pageSize) == 0 ? (size / pageSize) : (size / pageSize) + 1;
        if (size <= pageSize) {
            return list;
        }
        if (pageIndex > pageCount) {
            log.warn("当前页：{} 大于总页数：{}", pageIndex, pageCount);
            pageIndex = pageCount;
        }
        ArrayList<T> ob = new ArrayList<>();
        for (int i = (pageIndex - 1) * pageSize; i < Math.min(pageIndex * pageSize, size); i++) {
            ob.add(list.get(i));
        }
        return ob;
    }
}
