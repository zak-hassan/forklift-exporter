package dataformats.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;


public class AvroUtils<T> {



  /**
   * Reading avro schema files and creating a filewriter used to serialize data into avro format.
   * 
   * @param avroSchemaFileName
   * @param schemaSource
   * @return
   * @throws IOException
   */
  public static DataFileWriter<GenericRecord> createDataFileWriter(String avroSchemaFileName,
      Schema schemaSource) throws IOException {

    File file = new File(avroSchemaFileName);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schemaSource);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schemaSource, file);
    return dataFileWriter;
  }


  /**
   * Used to read beans from avro file store
   * 
   * @param avroSchemaFileName
   * @return
   * @throws IOException
   */
  public static <T> DataFileReader createDataFileReader(String avroSchemaFileName, Class<T> clazz)
      throws IOException {
    File file = new File(avroSchemaFileName);
    DatumReader<T> customerDR = new SpecificDatumReader<T>(clazz);
    DataFileReader<T> dataFR = new DataFileReader<T>(file, customerDR);
    return dataFR;
  }

}
