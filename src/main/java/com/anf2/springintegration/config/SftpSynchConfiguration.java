package com.anf2.springintegration.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;

import java.io.File;
import java.io.IOException;

@Configuration
@Slf4j
public class SftpSynchConfiguration {

    public DefaultSftpSessionFactory gimmeFactory(){
        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory();
        factory.setHost("documents.anf2.com");
        factory.setPort(22);
        factory.setAllowUnknownKeys(true);
        factory.setUser("ubuntu");
        factory.setPassword("password");
        return factory;
    }

    @Bean(name="mydefaultsync")
    public SftpInboundFileSynchronizer synchronizer(){
        SftpInboundFileSynchronizer sync = new SftpInboundFileSynchronizer(gimmeFactory());
        sync.setDeleteRemoteFiles(true);
        sync.setRemoteDirectory("/home/ubuntu/test1");
        sync.setFilter(new SftpSimplePatternFileListFilter("*.csv"));
        return sync;
    }

    @Bean(name="sftpMessageSource")
    @InboundChannelAdapter(channel="fileuploaded", poller = @Poller(fixedDelay = "30000"))
    public MessageSource<File> sftpMessageSource(){
        SftpInboundFileSynchronizingMessageSource source =
                new SftpInboundFileSynchronizingMessageSource(synchronizer());
        source.setLocalDirectory(new File("destination"));
        source.setAutoCreateLocalDirectory(true);
        source.setMaxFetchSize(1);
        return source;
    }


    @ServiceActivator(inputChannel = "fileuploaded")
    public void handleIncomingFile(File file) throws IOException {
        log.info(String.format("handleIncomingFile BEGIN %s", file.getName()));
        String content = FileUtils.readFileToString(file, "UTF-8");
        log.info(String.format("Content: %s", content));
        log.info(String.format("handleIncomingFile END %s", file.getName()));
    }


}
