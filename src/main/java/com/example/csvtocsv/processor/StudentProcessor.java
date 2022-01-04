package com.example.csvtocsv.processor;

import com.example.csvtocsv.dto.Student;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StudentProcessor implements ItemProcessor<Student,Student> {
    private static final Logger LOG = LoggerFactory.getLogger(StudentProcessor.class);

    @Override
    public Student process(Student item) throws Exception{
        final String firstName =  item.getFirstName().toUpperCase();
        if(item.getLastName().contains("a")) item.setLastName("JULIO");
        final String lastName =  item.getLastName().toUpperCase();
        Date dateNormal=new SimpleDateFormat("dd/MM/yyyy").parse(item.getFecha());
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dateFinal = dateFormat.format(dateNormal);
        final Student data =  new Student(item.getId(), firstName,lastName,item.getEmail(),dateFinal);

        LOG.info("Convirtiendo ("+item.getFirstName()+") a ("+data.getFirstName()+")");

        return data;
    }


}
