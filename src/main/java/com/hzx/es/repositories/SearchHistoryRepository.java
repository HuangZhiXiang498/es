package com.hzx.es.repositories;

import cn.hutool.core.util.IdUtil;
import com.hzx.es.entity.SearchHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SearchHistoryRepository extends ElasticsearchRepository<SearchHistory, String> {
    /**
     *  查询近30条历史记录
     * @param userId
     * @param keyword
     * @param pageable
     * @return
     */
    List<SearchHistory> findByUserIdAndKeywordContainingOrderBySearchTimeDesc(String userId,  String keyword, Pageable pageable);

    SearchHistory findByUserIdAndKeyword(String userId, String keyword);
    SearchHistory findByKeyword(String keyword);

    default SearchHistory upsertByUserIdAndKeyword(SearchHistory history) {
        SearchHistory existingHistory = findByUserIdAndKeyword(history.getUserId(), history.getKeyword());
        if (existingHistory == null) {
            history.setId(IdUtil.getSnowflakeNextId()+"");
            history.setSearchCount(1);
            return save(history);
        } else {
            existingHistory.setSearchCount(existingHistory.getSearchCount() + 1);
            return save(existingHistory);
        }
    }

    default SearchHistory upsertByKeyword(SearchHistory history) {
        SearchHistory existingHistory = findByKeyword(history.getKeyword());
        if (existingHistory == null) {
            history.setId(IdUtil.getSnowflakeNextId()+"");
            return save(history);
        }
        return existingHistory;
    }
}
