package com.example.testtask;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class CrptApi {

    private static final String SITE_URI = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    private final ObjectMapper mapper;


    private final TimeManager timeManager;

    private final DelayQueue<DelayedMessage> delayeds;


    public CrptApi(TimeUnit timeUnit, int requestLimit){
        mapper = new ObjectMapper();
        timeManager = new TimeManager(timeUnit, requestLimit);
        delayeds = new DelayQueue<>();
        new Thread(new DelayManager()).start();
    }


    public void createDocument(DocumentDto document, String sign){
        try {
            create(document, sign);
            System.out.println("Document created: " + sign);
        } catch (TimeException e){
            delayeds.add(new DelayedMessage(System.currentTimeMillis()+e.getMillis(),document,sign));
        }
    }

    private synchronized void create(DocumentDto document, String sign){
        timeManager.check();
        try(CloseableHttpClient httpClient = HttpClientBuilder.create().build();) {
            HttpPost request = new HttpPost(SITE_URI);
            StringEntity params = new StringEntity(mapper.writeValueAsString(document));
            request.addHeader("content-type", "application/x-www-form-urlencoded");
            request.setEntity(params);
            // Не до конца понятно, чем именно является "подпись". Предположим, что это токен для аутентификации
            request.setHeader("Authorization", sign);
            HttpResponse response = httpClient.execute(request);
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }


    }


    private class DelayManager implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    DelayedMessage delayedMessage = delayeds.take();
                    createDocument(delayedMessage.getDocument(), delayedMessage.getSign());
                }
                catch (InterruptedException e){}
            }
        }
    }

    private static class DelayedMessage implements Delayed{

        private final long startTime;
        private final DocumentDto document;
        private final String sign;

        public DocumentDto getDocument() {
            return document;
        }

        public String getSign() {
            return sign;
        }

        private DelayedMessage(long startTime, DocumentDto document, String sign){
            this.startTime = startTime;
            this.document = document;
            this.sign = sign;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = startTime - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return (int) (this.startTime - ((DelayedMessage) o).startTime);
        }
    }

    private static class TimeManager{

        private final long step;
        private final int requestLimit;

        private final Queue<Long> queue;

        private TimeManager(TimeUnit timeUnit, int requestLimit){
            step = timeUnit.toMillis(1);
            this.requestLimit = requestLimit;
            queue = new LinkedList<>();
        }

        synchronized void check(){
            while (!queue.isEmpty() && System.currentTimeMillis() > queue.peek()){
                queue.poll();
            }
            if (queue.size()<requestLimit){
                queue.add(System.currentTimeMillis() + step);
            }
            else {
                throw new TimeException(queue.peek() - System.currentTimeMillis());
            }
        }



    }


    private static class TimeException extends RuntimeException{
        private final long millis;

        private TimeException(long millis){
            this.millis = millis;
        }

        public long getMillis() {
            return millis;
        }
    }


    public static class Description{
        public String participantInn;
    }

    public static class Product{
        public String certificate_document;
        public String certificate_document_date;
        public String certificate_document_number;
        public String owner_inn;
        public String producer_inn;
        public String production_date;
        public String tnved_code;
        public String uit_code;
        public String uitu_code;
    }

    public static class DocumentDto{
        public Description description;
        public String doc_id;
        public String doc_status;
        public String doc_type;
        public boolean importRequest;
        public String owner_inn;
        public String participant_inn;
        public String producer_inn;
        public String production_date;
        public String production_type;
        public ArrayList<Product> products;
        public String reg_date;
        public String reg_number;
    }

}
