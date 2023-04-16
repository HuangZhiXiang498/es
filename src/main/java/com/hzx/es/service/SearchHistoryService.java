package com.hzx.es.service;

import com.hzx.es.entity.SearchHistory;
import com.hzx.es.repositories.SearchHistoryRepository;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class SearchHistoryService {

    @Autowired
    private SearchHistoryRepository searchHistoryRepository;
    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    public List<SearchHistory> searchRecentHistory(String userId, String keyword) {
        Pageable pageable = PageRequest.of(0, 30, Sort.by(Sort.Direction.DESC, "searchTime"));
        return searchHistoryRepository.findByUserIdAndKeywordContainingOrderBySearchTimeDesc(userId, keyword, pageable);
    }


    public SearchHistory addOrUpdateByUserIdAndKeyword(SearchHistory history) {
        return searchHistoryRepository.upsertByUserIdAndKeyword(history);
    }
    public SearchHistory addOrUpdateByKeyword(SearchHistory history) {

        return searchHistoryRepository.upsertByKeyword(history);
    }


    public List<String> findTop30KeywordsWithMaxCount() throws IOException {

        String groupByKeyword = "group_by_keyword";
        String sumCount = "sum_count";
        Query searchQuery = new NativeSearchQueryBuilder()
                .withSearchType(SearchType.DEFAULT)
                .addAggregation(
                        AggregationBuilders.terms(groupByKeyword)
                                .field("keyword")
                                .subAggregation(AggregationBuilders.sum(sumCount).field("count")))
                .build();

        SearchHits<SearchHistory> searchHits = elasticsearchRestTemplate.search(searchQuery, SearchHistory.class);
        Aggregations aggregations = searchHits.getAggregations();
        Terms keywordTerms = aggregations.get(groupByKeyword);
        List<? extends Terms.Bucket> keywordBuckets = keywordTerms.getBuckets();

        return keywordBuckets.stream()
                .map(b -> (Terms.Bucket) b)
                .sorted(Comparator.comparingDouble(b -> ((Sum) (((Terms.Bucket) b).getAggregations().get(sumCount))).value())
                        .reversed())
                .limit(30)
                .map(Terms.Bucket::getKeyAsString)
                .collect(Collectors.toList());
    }

    public List<String> searchWithAutoCorrect(String keyword) {
        FuzzyQueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("keyword", keyword)
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);
        Query searchQuery =  new NativeSearchQueryBuilder().withQuery(queryBuilder).build();
        SearchHits<SearchHistory> searchHits = elasticsearchRestTemplate.search(searchQuery, SearchHistory.class);
        return searchHits.stream()
                .map(b -> (Terms.Bucket) b)
                .map(Terms.Bucket::getKeyAsString)
                .limit(30)
                .collect(Collectors.toList());

    }

}
