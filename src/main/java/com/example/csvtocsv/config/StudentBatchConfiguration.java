package com.example.csvtocsv.config;


import com.example.csvtocsv.dto.Student;
import com.example.csvtocsv.listener.MyChunkListener;
import com.example.csvtocsv.processor.StudentProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class StudentBatchConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public FlatFileItemReader<Student> readDataFromCsv() {

        FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("C://Users/User/Documents/data/csv_input.csv"));
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper<Student>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(Student.fields());

                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {
                    {
                        setTargetType(Student.class);

                    }
                });
            }
        });

        return reader;
    }

    @Bean
    public StudentProcessor processor(){
        return new StudentProcessor();
    }

    @Bean
    public FlatFileItemWriter<Student> writer(){
        String data="id||nombre||apellido||email||fecha+";
        StringHeaderWriter stringHeaderWriter = new StringHeaderWriter(data);
        FlatFileItemWriter<Student> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("C://Users/User/Documents/data/csv_output.csv"));
        DelimitedLineAggregator<Student> aggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<Student> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(Student.fields());
        aggregator.setFieldExtractor(extractor);
        aggregator.setDelimiter("||");
        writer.setLineAggregator(aggregator);
        writer.setHeaderCallback(stringHeaderWriter);



        return  writer;
    }


    @Bean
    public Step executeStudentStep(){
        return stepBuilderFactory.get("executeStudentStep")
                .<Student,Student>chunk(3)
                .reader(readDataFromCsv())
                .processor(processor())
                .writer(writer())
                .listener(new MyChunkListener())
                .build();
    }

    @Bean
    public Job processStudentJob(){
        return jobBuilderFactory.get("processStudentJob").flow(executeStudentStep()).end().build();
    }


}