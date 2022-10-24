package com.riu.crs.poc_price_explosion_updates;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.result.UpdateResult;
import com.riu.crs.poc_price_explosion_updates.dto.CloseSaleDto;
import com.riu.crs.poc_price_explosion_updates.dto.NewContractInventoryDto;
import com.riu.crs.poc_price_explosion_updates.dto.NewContractPriceDto;
import com.riu.crs.poc_price_explosion_updates.dto.PriceDto;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@SpringBootApplication
public class PocPriceExplosionUpdatesApplication {

    private static final String TOPIC_PRICE = "denormalized_basic_pricing_data";
    private static final String TOPIC_INVENTORY = "denormalized_inventory_data";
    private static final Gson gson = new Gson();
    private static final String pattern = "yyyy/MM/dd";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(pattern);
    private static MongoTemplate mongoTemplate;

    public static void main(String[] args) {
        SpringApplication.run(PocPriceExplosionUpdatesApplication.class, args);
        mongoTemplate = getMongoTemplate(args[1], args[2]);
        int partitionNumber = args.length > 3 && !args[3].equalsIgnoreCase("0") ? Integer.parseInt(args[3]) : 12;
        Consumer<Long, String> consumerPrice = null;
        Consumer<Long, String> consumerInventory = null;
        ExecutorService executor = Executors.newFixedThreadPool(partitionNumber * 2);
        for (int i = 0; i < partitionNumber; i++) {
            consumerPrice = createConsumer(TOPIC_PRICE, "price_consumer", args[0]);
            consumerInventory = createConsumer(TOPIC_INVENTORY, "inventory_consumer", args[0]);
            executor.execute(new ReceivePriceMessage(consumerPrice));
            executor.execute(new ReceiveInventoryMessage(consumerInventory));
        }
    }

    static class ReceiveInventoryMessage implements Runnable {
        Consumer<Long, String> consumer;

        public ReceiveInventoryMessage(Consumer<Long, String> consumer) {
            this.consumer = consumer;
        }

        @SneakyThrows
        public void run() {
            while (true) {
                ConsumerRecords<Long, String> recs = consumer.poll(Duration.ofMillis(0));
                if (!recs.isEmpty()) {
                    List<Pair<Query, Update>> pairs = new ArrayList<>();
                    recs.iterator().forEachRemaining(r -> {
                        try {
                            log.info("receive inventory message: " + r.toString());
                            pairs.addAll(updateInventory(gson.fromJson(r.value(), NewContractInventoryDto.class)));
                        } catch (ParseException e) {
                            log.error(e.getMessage());
                        }
                    });
                    BulkOperations bulk = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, "poc");
                    bulk.updateMulti(pairs);
                    log.info("Inventory documents updated: " + bulk.execute().getModifiedCount());
                    consumer.commitSync();
                }
            }
        }
    }

    static class ReceivePriceMessage implements Runnable {
        Consumer<Long, String> consumer;

        public ReceivePriceMessage(Consumer<Long, String> consumer) {
            this.consumer = consumer;
        }

        @SneakyThrows
        public void run() {
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(0));
                List<Pair<Query, Update>> pairs = new ArrayList<>();
                if (!records.isEmpty()) {
                    records.iterator().forEachRemaining(r -> {
                        try {
                            log.info("receive price message: " + r.toString());
                            gson.fromJson(r.value(), JsonObject.class);
                            if (gson.fromJson(r.value(), JsonObject.class).get("closingSale") != null) {
                                pairs.addAll(updateCloseSale(gson.fromJson(r.value(), CloseSaleDto.class)));
                            } else {
                                pairs.addAll(updatePrice(gson.fromJson(r.value(), NewContractPriceDto.class)));
                            }
                        } catch (ParseException e) {
                            log.error(e.getMessage());
                        }
                    });
                    BulkOperations bulk = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, "poc");
                    bulk.updateMulti(pairs);
                    log.info("Price documents updated: " + bulk.execute().getModifiedCount());
                    consumer.commitSync();
                }
            }
        }
    }

    private static Consumer<Long, String> createConsumer(String topic, String group, String servers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                group);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                10485);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private static synchronized List<Pair<Query, Update>> updatePrice(NewContractPriceDto price) throws ParseException {
        Date fromDay = sdf.parse(price.getFromDay());
        Date toDay = sdf.parse(price.getToDay());
        List<Pair<Query, Update>> pairs = new ArrayList<>();
        while (toDay.compareTo(fromDay) == 1) {
            Query query = new Query();
            query.addCriteria(Criteria.where("hotelId").is(price.getHotelId()));
            query.addCriteria(Criteria.where("rateId").is(price.getRateId()));
            query.addCriteria(Criteria.where("roomId").is(price.getRoomId()));
            query.addCriteria(Criteria.where("stayDate").is(sdf.format(fromDay)));
            Update update = new Update();
            PriceDto.PriceDetail detail = price.getPriceDetail();
            update.set("priceDetail.totalNet", detail.getTotalNet());
            update.set("priceDetail.totalTax", detail.getTotalTax());
            update.set("priceDetail.totalGross", detail.getTotalGross());
            update.set("priceDetail.totalVax", detail.getTotalVax());
            update.set("priceDetail.currency", detail.getCurrency());
            pairs.add(Pair.of(query, update));
            fromDay = addDayToDate(fromDay);
        }
        return pairs;
    }

    private static synchronized List<Pair<Query, Update>> updateCloseSale(CloseSaleDto closeSaleDto) {
        Query query = new Query();
        query.addCriteria(Criteria.where("hotelId").is(closeSaleDto.getHotelId()));
        query.addCriteria(Criteria.where("rateId").is(closeSaleDto.getRateId()));
        Update update = new Update();
        update.set("closingSale", closeSaleDto.isClosingSale());
        return Collections.singletonList(Pair.of(query, update));
    }

    private static MongoTemplate getMongoTemplate(String connectionString, String databaseName) {
        MongoClient mongoClient = MongoClients.create(connectionString);
        return new MongoTemplate(mongoClient, databaseName);
    }

    private static synchronized List<Pair<Query, Update>> updateInventory(NewContractInventoryDto inventory) throws ParseException {
        Query query = new Query();
        query.addCriteria(Criteria.where("hotelId").is(inventory.getHotelId()));
        query.addCriteria(Criteria.where("rateId").is(inventory.getRateId()));
        query.addCriteria(Criteria.where("roomId").is(inventory.getRoomId()));
        query.addCriteria(Criteria.where("stayDate").is(inventory.getStayDate()));
        Update update = new Update();
        update.set("inventory", inventory.getInventory());
        return Collections.singletonList(Pair.of(query, update));
    }

    private static Date addDayToDate(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, 1);
        return cal.getTime();
    }

}
