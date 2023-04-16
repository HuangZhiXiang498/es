package com.hzx.es;

import cn.hutool.core.util.RandomUtil;
import com.github.javafaker.Faker;
import com.hzx.es.entity.SearchHistory;
import com.hzx.es.service.SearchHistoryService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

@SpringBootTest
@Slf4j
public class SearchHistoryServiceTest {
    Faker faker = new Faker(Locale.CHINA);
    @Autowired
    private SearchHistoryService searchHistoryService;


    @Test
    public void save() throws Exception {
        for (int i = 0; i < 1000; i++) {
            searchHistoryService.addOrUpdateByKeyword(
                    SearchHistory.builder()
                            .searchTime(LocalDateTime.now())
                            .keyword(faker.name().name())
                            .build()
            );
            for (int i1 = 0; i1 < 10; i1++) {
                searchHistoryService.addOrUpdateByUserIdAndKeyword(
                        SearchHistory.builder()
                                .searchTime(LocalDateTime.now())
                                .keyword(faker.name().name())
                                .userId(i+"")
                                .build()
                );
            }
        }
    }

    @Test
    public void search() throws Exception {
        List<SearchHistory> searchHistories = searchHistoryService.searchRecentHistory(
                RandomUtil.randomInt(0, 1000) + "",
                "黄"
        );
        List<String> keywords = searchHistoryService.searchWithAutoCorrect("黄");
        List<String> keywordsWithMaxCount = searchHistoryService.findTop30KeywordsWithMaxCount();
    }
}
