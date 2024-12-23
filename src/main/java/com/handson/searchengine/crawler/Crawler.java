package com.handson.searchengine.crawler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.searchengine.kafka.Producer;
import com.handson.searchengine.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

@Service
public class Crawler {

    @Autowired
    RedisTemplate redisTemplate;

    @Autowired
    ObjectMapper om;

    @Autowired
    Producer producer;

    protected final Log logger = LogFactory.getLog(getClass());

    private int curDistance = 0;  // Added curDistance
    private long startTime = 0;   // Added startTime
    private BlockingQueue<CrawlerRecord> queue = new ArrayBlockingQueue<>(100000); // Added queue

    public CrawlStatus Crawler(String crawlId, CrawlerRequest crawlerRequest) throws InterruptedException, JsonProcessingException {
        initCrawlInRedis(crawlId);
        producer.send(CrawlerRecord.of(crawlId, crawlerRequest));
        curDistance = 0; // Initialize curDistance
        startTime = System.currentTimeMillis(); // Initialize startTime

        StopReason stopReason = null; // Added stopReason
        return CrawlStatus.of(curDistance, startTime, getVisitedUrls(crawlId), stopReason);
    }

    public void crawlOneRecord(String crawlId, CrawlerRecord rec) throws IOException, InterruptedException {
        logger.info("Crawling URL: " + rec.getUrl());
        StopReason stopReason = getStopReason(rec); // Added stopReason initialization
        setCrawlStatus(crawlId, CrawlStatus.of(rec.getDistance(), System.currentTimeMillis(), 0, stopReason));
        if (stopReason == null) {
            Document webPageContent = Jsoup.connect(rec.getUrl()).get(); // Removed .get() from String
            List<String> innerUrls = extractWebPageUrls(rec.getBaseUrl(), webPageContent);
            addUrlsToQueue(rec, innerUrls, rec.getDistance() + 1);
        }
    }

    private StopReason getStopReason(CrawlerRecord rec) {
        if (rec.getDistance() == rec.getMaxDistance() + 1) return StopReason.maxDistance;
        if (getVisitedUrls(rec.getCrawlId()) >= rec.getMaxUrls()) return StopReason.maxUrls;
        if (System.currentTimeMillis() >= rec.getMaxTime()) return StopReason.timeout;
        return null;
    }

    private void addUrlsToQueue(CrawlerRecord rec, List<String> urls, int distance) throws InterruptedException, JsonProcessingException {
        logger.info(">> Adding URLs to queue: distance -> " + distance + ", amount -> " + urls.size());
        for (String url : urls) {
            if (!crawlHasVisited(rec, url)) {
                producer.send(CrawlerRecord.of(rec).withUrl(url).withIncDistance());
            }
        }
    }

    private List<String> extractWebPageUrls(String baseUrl, Document webPageContent) {
        List<String> links = webPageContent.select("a[href]")
                .eachAttr("abs:href")
                .stream()
                .filter(url -> url.startsWith(baseUrl)) // Filter links within the same base URL
                .collect(Collectors.toList());
        logger.info(">> Extracted -> " + links.size() + " links");
        return links;
    }

    private boolean isValidUrl(String url) {
        try {
            new java.net.URL(url).toURI(); // Validates URL syntax
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Document fetchWithRetry(String url, int retries) {
        for (int i = 0; i < retries; i++) {
            try {
                return Jsoup.connect(url)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                        .timeout(5000) // Timeout in milliseconds
                        .get();
            } catch (IOException e) {
                logger.warn("Attempt " + (i + 1) + " failed for URL: " + url + " - " + e.getMessage());
            }
        }
        return null; // Return null if all retries fail
    }

    private void initCrawlInRedis(String crawlId) throws JsonProcessingException {
        setCrawlStatus(crawlId, CrawlStatus.of(0, System.currentTimeMillis(), 0, null));
        redisTemplate.opsForValue().set(crawlId + ".urls.count", "1");
    }

    private void setCrawlStatus(String crawlId, CrawlStatus crawlStatus) throws JsonProcessingException {
        redisTemplate.opsForValue().set(crawlId + ".status", om.writeValueAsString(crawlStatus));
    }

    private boolean crawlHasVisited(CrawlerRecord rec, String url) {
        if (redisTemplate.opsForValue().setIfAbsent(rec.getCrawlId() + ".urls." + url, "1")) {
            redisTemplate.opsForValue().increment(rec.getCrawlId() + ".urls.count", 1L);
            return false;
        }
        return true;
    }

    private int getVisitedUrls(String crawlId) {
        Object curCount = redisTemplate.opsForValue().get(crawlId + ".urls.count");
        if (curCount == null) return 0;
        return Integer.parseInt(curCount.toString());
    }

    public CrawlStatusOut getCrawlInfo(String crawlId) throws JsonProcessingException {
        CrawlStatus cs = om.readValue(redisTemplate.opsForValue().get(crawlId + ".status").toString(), CrawlStatus.class);
        cs.setNumPages(getVisitedUrls(crawlId));
        return CrawlStatusOut.of(cs);
    }
}
