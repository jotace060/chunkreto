package com.example.csvtocsv.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.io.IOException;

public class MyChunkListener implements ChunkListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private long startTimeInMs;

    @Override
    public void beforeChunk(ChunkContext context) {
        startTimeInMs = System.currentTimeMillis();
        logger.info("Iniciando procesamiento por pedazo");
    }

    @Override
    public void afterChunk(ChunkContext context) {
        long duration = System.currentTimeMillis() - startTimeInMs;
        long chunkSize = 3;
        logger.info(String.format(
                "Chunk of size %s computed in %sms", chunkSize, duration));
        logger.info("Finalizando procesamiento por pedazo");

        // Lectura de hasta 10 bytes
        byte [] buffer = new byte[10];
        try {
            System.in.read(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {

    }

}