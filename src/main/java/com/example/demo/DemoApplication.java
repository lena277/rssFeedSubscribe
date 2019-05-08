package com.example.demo;

import com.rometools.rome.feed.synd.SyndEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.feed.dsl.Feed;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;
import org.springframework.integration.transformer.AbstractPayloadTransformer;
import org.springframework.messaging.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executor;

@SpringBootApplication
@Configuration
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Value("${rssFeed.path}")
    private Resource feedResource;

    @Value("${outputDerictory.path}")
    private String outputDirecory;

    public File tempFolder = new File("");

    @Bean
    public MetadataStore metadataStore() {
        PropertiesPersistingMetadataStore metadataStore = new PropertiesPersistingMetadataStore();
        metadataStore.setBaseDirectory(tempFolder.getAbsolutePath());
        return metadataStore;
    }
    public Executor getExecutor()
    {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(20);
        executor.initialize();
        return executor;
    }
    @Bean
    public MessageChannel queueChannel() {
        return MessageChannels.queue().get();
    }

    @Bean
    public MessageChannel publishSubscribe() {
        return MessageChannels.publishSubscribe().get();
    }
    private static final Logger logger  = LoggerFactory.getLogger(DemoApplication.class);

    @Bean
    public IntegrationFlow feedFlow() {
        return IntegrationFlows
                .from(Feed.inboundAdapter(this.feedResource, "feedTest")
                                .metadataStore(metadataStore()),
                        e -> e.poller(p -> p.fixedDelay(100))).transform(extractLinkFromFeed())
                .<String>handle((p, h) -> fileWriter(h.getId().toString()))
                .handle(System.out::println)
                .get();
    }


    public FileWritingMessageHandler fileWriter(String item) {
        FileWritingMessageHandler writer = new FileWritingMessageHandler(

                new File("C://Users//second acc//" +
                        "IdeaProjects//demo//src//main//resources//output//7-5-2019//"+item+".xml"));
        writer.setExpectReply(false);
        writer.setFileExistsMode(FileExistsMode.APPEND);
        writer.setAppendNewLine(true);
        return writer;
    }



    @Bean
    public AbstractPayloadTransformer<SyndEntry, String> extractLinkFromFeed() {
        return new AbstractPayloadTransformer<SyndEntry, String>() {
            @Override
            protected String transformPayload(SyndEntry payload) throws Exception {
                String dateStr =  payload.getPublishedDate().toString();
                DateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
                Date date = (Date)formatter.parse(dateStr);
                System.out.println(date);

                Calendar cal = Calendar.getInstance();
                cal.setTime(date);
                String formatedDate = cal.get(Calendar.DATE) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" +         cal.get(Calendar.YEAR);
                createDir(formatedDate,payload.getCategories().get(0).getName() );
                return payload.getComments();
            }
        };

    }
    //    @Bean
//    public MessageHandler targetDirectory() {
//        FileWritingMessageHandler handler = new FileWritingMessageHandler(new File(OUTPUT_DIR));
//        handler.setFileExistsMode(FileExistsMode.REPLACE);
//        handler.setExpectReply(false);
//        return handler;
//    }
    public void createDir(String name){
        System.out.println(name + " lllllllll");

        if(!name.startsWith(outputDirecory)) {
            name =  outputDirecory+"/"+ name;

        }
        File theDir = new File(name);

        if (!theDir.exists()) {
            try{
                theDir.mkdir();
            }
            catch(SecurityException se){
                //handle it
            }

        }

    }
    public void createDir(String name, String sub){
        File subDir = null;
        if(!new File(outputDirecory+"/"+name+"/"+name).exists())
            createDir(name);

        subDir = new File(outputDirecory+"/"+name+"/"+sub);

        if (!subDir.exists()) {
            boolean result = false;

            try{
                subDir.mkdir();
                result = true;
            }
            catch(SecurityException se){
                //handle it
            }
            if(result) {
                System.out.println("DIR created");
            }
        }

    }



}
