package com.example.csvtocsv;

import com.example.csvtocsv.dto.Student;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;


@RunWith(MockitoJUnitRunner.class)
public class CsvTocsvApplicationTests {


    // reads the output file to check the result
    BufferedReader reader;

    @Test(expected = IllegalArgumentException.class)
    public void testMissingLineAggregator() {
        new FlatFileItemWriterBuilder<Student>()
                .build();
    }

    @Test(expected = IllegalStateException.class)
    public void testMultipleLineAggregators() throws IOException {
        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        new FlatFileItemWriterBuilder<Student>()
                .name("itemWriter")
                .resource(output)
                .delimited()
                .delimiter(";")
                .names("student", "bar")
                .formatted()
                .format("%2s%2s")
                .names("student", "bar")
                .build();
    }

    @Test
    public void test() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .resource(output)
                .lineSeparator("$")
                .lineAggregator(new PassThroughLineAggregator<>())
                .encoding("UTF-16LE")
                .headerCallback(writer1 -> writer1.append("HEADER"))
                .footerCallback(writer12 -> writer12.append("FOOTER"))
                .build();

        ExecutionContext executionContext = new ExecutionContext();

        writer.open(executionContext);
        writer.write(Arrays.asList(new Student(1,"julio","osorio","test@gmail.com","01/03/1997"),
                new Student(2,"cesar","rodriguez","fo@gmail.com","02/01/2021")));

        writer.close();
        assertEquals("HEADER$Student{id=1, firstName='julio', lastName='osorio', email='test@gmail.com', fecha='01/03/1997'}" +
                "$Student{id=2, firstName='cesar', lastName='rodriguez', email='fo@gmail.com', fecha='02/01/2021'}$FOOTER", readLine("UTF-16LE", output));
    }

    @Test
    public void testDelimitedOutputWithDefaultDelimiter() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .resource(output)
                .lineSeparator("$")
                .delimited()
                .names("id","firstName","lastName","email","fecha")
                .encoding("UTF-16LE")
                .headerCallback(writer1 -> writer1.append("HEADER"))
                .footerCallback(writer12 -> writer12.append("FOOTER"))
                .build();

        ExecutionContext executionContext = new ExecutionContext();

        writer.open(executionContext);

        writer.write(Arrays.asList(new Student(1,"julio","osorio","test@gmail.com","01/03/1997"),
                new Student(2,"cesar","rodriguez","fo@gmail.com","02/01/2021")));

        writer.close();

        assertEquals("HEADER$1,julio,osorio,test@gmail.com,01/03/1997$2,cesar,rodriguez,fo@gmail.com,02/01/2021$FOOTER", readLine("UTF-16LE", output));
    }

    @Test
    public void testDelimitedOutputWithEmptyDelimiter() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .resource(output)
                .lineSeparator("$")
                .delimited()
                .delimiter("")
                .names("id","firstName","lastName","email","fecha")
                .encoding("UTF-16LE")
                .headerCallback(writer1 -> writer1.append("HEADER"))
                .footerCallback(writer12 -> writer12.append("FOOTER"))
                .build();

        ExecutionContext executionContext = new ExecutionContext();

        writer.open(executionContext);

        writer.write(Arrays.asList(new Student(1,"julio","osorio","test@gmail.com","01/03/1997"),
                new Student(2,"cesar","rodriguez","fo@gmail.com","02/01/2021")));

        writer.close();

        assertEquals("HEADER$1julioosoriotest@gmail.com01/03/1997$2cesarrodriguezfo@gmail.com02/01/2021$FOOTER", readLine("UTF-16LE", output));
    }

    @Test
    public void testDelimitedOutputWithDefaultFieldExtractor() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .resource(output)
                .lineSeparator("$")
                .delimited()
                .delimiter(";")
                .names("id","firstName","lastName","email","fecha")
                .encoding("UTF-16LE")
                .headerCallback(writer1 -> writer1.append("HEADER"))
                .footerCallback(writer12 -> writer12.append("FOOTER"))
                .build();

        ExecutionContext executionContext = new ExecutionContext();

        writer.open(executionContext);


        writer.write(Arrays.asList(new Student(1,"julio","osorio","test@gmail.com","01/03/1997"),
                new Student(2,"cesar","rodriguez","fo@gmail.com","02/01/2021")));

        writer.close();

        assertEquals("HEADER$1;julio;osorio;test@gmail.com;01/03/1997$2;cesar;rodriguez;fo@gmail.com;02/01/2021$FOOTER", readLine("UTF-16LE", output));
    }


    @Test
    public void testDelimitedOutputWithCustomFieldExtractor() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .resource(output)
                .lineSeparator("$")
                .delimited()
                .delimiter(" ")
                .fieldExtractor(item -> new Object[] {item.getId(), item.getFirstName(),item.getLastName(),item.getEmail(),item.getFecha()})
                .encoding("UTF-16LE")
                .headerCallback(writer1 -> writer1.append("HEADER"))
                .footerCallback(writer12 -> writer12.append("FOOTER"))
                .build();

        ExecutionContext executionContext = new ExecutionContext();

        writer.open(executionContext);

        writer.write(Arrays.asList(new Student(1,"julio","osorio","test@gmail.com","01/03/1997"),
                new Student(2,"cesar","rodriguez","fo@gmail.com","02/01/2021")));

        writer.close();
        assertEquals("HEADER$1 julio osorio test@gmail.com 01/03/1997$2 cesar rodriguez fo@gmail.com 02/01/2021$FOOTER", readLine("UTF-16LE", output));
    }

    @Test
    public void testFormattedOutputWithDefaultFieldExtractor() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .resource(output)
                .lineSeparator("$")
                .formatted()
                .format("%2s%2s%2s")
                .names("id","firstName","lastName","email","fecha")
                .encoding("UTF-16LE")
                .headerCallback(writer1 -> writer1.append("HEADER"))
                .footerCallback(writer12 -> writer12.append("FOOTER"))
                .build();

        ExecutionContext executionContext = new ExecutionContext();

        writer.open(executionContext);

        writer.write(Arrays.asList(new Student(1,"julio","osorio","test@gmail.com","01/03/1997"),
                new Student(2,"cesar","rodriguez","fo@gmail.com","02/01/2021")));

        writer.close();

        assertEquals("HEADER$ 1julioosorio$ 2cesarrodriguez$FOOTER", readLine("UTF-16LE", output));
    }

    @Test
    public void testFlags() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));

        String encoding = Charset.defaultCharset().name();

        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .resource(output)
                .shouldDeleteIfEmpty(true)
                .shouldDeleteIfExists(false)
                .saveState(false)
                .forceSync(true)
                .append(true)
                .transactional(false)
                .lineAggregator(new PassThroughLineAggregator<>())
                .build();

        validateBuilderFlags(writer, encoding);
    }

    @Test
    public void testFlagsWithEncoding() throws Exception {

        Resource output = new FileSystemResource(File.createTempFile("student", "txt"));
        String encoding = "UTF-8";
        FlatFileItemWriter<Student> writer = new FlatFileItemWriterBuilder<Student>()
                .name("student")
                .encoding(encoding)
                .resource(output)
                .shouldDeleteIfEmpty(true)
                .shouldDeleteIfExists(false)
                .saveState(false)
                .forceSync(true)
                .append(true)
                .transactional(false)
                .lineAggregator(new PassThroughLineAggregator<>())
                .build();
        validateBuilderFlags(writer, encoding);
    }



    private void validateBuilderFlags(FlatFileItemWriter<Student> writer, String encoding) {
        assertFalse((Boolean) ReflectionTestUtils.getField(writer, "saveState"));
        assertTrue((Boolean) ReflectionTestUtils.getField(writer, "append"));
        assertFalse((Boolean) ReflectionTestUtils.getField(writer, "transactional"));
        assertTrue((Boolean) ReflectionTestUtils.getField(writer, "shouldDeleteIfEmpty"));
        assertFalse((Boolean) ReflectionTestUtils.getField(writer, "shouldDeleteIfExists"));
        assertTrue((Boolean) ReflectionTestUtils.getField(writer, "forceSync"));
        assertEquals( encoding, ReflectionTestUtils.getField(writer, "encoding"));
    }


    private String readLine(String encoding, Resource outputFile ) throws IOException {

        if (reader == null) {
            reader = new BufferedReader(new InputStreamReader(outputFile.getInputStream(), encoding));
        }

        return reader.readLine();
    }


}
