package io.bigconnect.dw.text.search;

import lombok.Getter;

import java.util.List;
import java.util.UUID;
import java.time.LocalDateTime;

@Getter
public class CombinedSearchRequest {
    private String query;
    private int limit;
    private float threshold;
    private boolean search_keywords;
    private boolean search_buckets;

    public CombinedSearchRequest(String query, int limit, float threshold, boolean search_keywords, boolean search_buckets) {
        this.query = query;
        this.limit = limit;
        this.threshold = threshold;
        this.search_keywords = search_keywords;
        this.search_buckets = search_buckets;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    public void setSearch_keywords(boolean search_keywords) {
        this.search_keywords = search_keywords;
    }

    public void setSearch_buckets(boolean search_buckets) {
        this.search_buckets = search_buckets;
    }
}