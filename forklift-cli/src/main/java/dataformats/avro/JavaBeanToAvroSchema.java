package dataformats.avro;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import dataformats.model.CustomerBean;
import dataformats.utils.SchemaUtils;

/**
 * Demonstrating conversion of normal java pojo infer the avro schema for the pojo.
 * 
 * @author Zak Hassan <zak.hassan@redhat.com>
 */
public class JavaBeanToAvroSchema {

  public JavaBeanToAvroSchema() {
    Writer writer = null;
    try {
      // You can also choose to use the class name to generate your schema if the class doesn't yet
      // exist.
      // For example: SchemaUtils.toAvroSchema("com.redhat.data.analytics.model.CustomerBean");
      Schema avroSchema = SchemaUtils.toAvroSchema(CustomerBean.class);
      String asJson = avroSchema.toString(true);
    
      System.out.println("Schema: " + asJson);
      writer = new FileWriter("src/main/resources/schema.json");
      writer.write(asJson);

    } catch (ClassNotFoundException | IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          System.err.println("Error: Could not write avro schema to file.");;
          e.printStackTrace();
        }
      }
    }

  }

 


  public static void main(String[] args) {
    new JavaBeanToAvroSchema();

  }
}
