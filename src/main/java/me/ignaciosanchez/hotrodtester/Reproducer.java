package me.ignaciosanchez.hotrodtester;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.util.Random;

@Configuration
public class Reproducer {

    @Value("${datagrid.host}")
    private String host;
    @Value("${datagrid.port}")
    private int port;
    @Value("${datagrid.cache-name}")
    private String cacheName;
    @Value("${datagrid.entry-size}")
    private int entrySize;
    @Value("${datagrid.number-of-iterations}")
    private int numberOfIterations;

    private RemoteCacheManager remoteCacheManager;

    Logger logger = LoggerFactory.getLogger(Reproducer.class);

    @EventListener(ApplicationReadyEvent.class)
    public void tester() {

        String xml = String.format(
                "<infinispan>" +
                        "   <cache-container>" +
                        "       <distributed-cache name=\"%s\" mode=\"SYNC\" owners=\"1\" statistics=\"true\">" +
//                        "           <encoding>" +
//                        "               <key media-type=\"application/x-protostream\"/>" +
//                        "               <value media-type=\"application/x-protostream\"/>" +
//                        "           </encoding>" +
//                    "           <transaction mode=\"NONE\"/>" +
//                    "           <expiration lifespan=\"-1\" max-idle=\"-1\" interval=\"60000\"/>" +
//                    "           <memory storage=\"HEAP\"/>" +
//                    "           <indexing enabled=\"true\">" +
//                    "               <key-transformers/>" +
//                    "               <indexed-entities/>" +
//                    "           </indexing>" +
//                    "           <state-transfer enabled=\"false\" await-initial-transfer=\"false\"/>" +
//                    "           <partition-handling when-split=\"ALLOW_READ_WRITES\" merge-policy=\"REMOVE_ALL\"/>" +
                        "       </distributed-cache>" +
                        "   </cache-container>" +
                        "</infinispan>", cacheName);

        org.infinispan.client.hotrod.configuration.Configuration configuration = new ConfigurationBuilder()
                .statistics()
                    .enable()
                .addServer()
                    .host(host)
                    .port(port)
                .build();

        byte[] bytes = new byte[entrySize];
        Random rnd = new Random();

        this.remoteCacheManager = new RemoteCacheManager(configuration);

        logger.info("Available caches: " + remoteCacheManager.getCacheNames());

//        remoteCacheManager.administration().removeCache(cacheName);

        RemoteCache<String, byte[]> cache = remoteCacheManager.administration()
                .getOrCreateCache(cacheName,  new XMLStringConfiguration(xml));

        logger.info("\n--> Test begins <--\n");
        logger.info("Available caches: " + remoteCacheManager.getCacheNames());
        logger.info("Content of entry #80: " + cache.get("80") + "\n");

        int iterationNumber = 0;

        while (iterationNumber++ < numberOfIterations) {
            logger.info("Iteration " + iterationNumber);
            logger.info("Put entry #80");
            try {
                cache.put("80", bytes);
            } catch (Exception e) {
                //TODO: handle exception
                logger.error("There was an error putting the object on the remote cache", e);
            }
        }
        logger.info("Content of entry #80: " + cache.get("80") + "\n");
    }
}