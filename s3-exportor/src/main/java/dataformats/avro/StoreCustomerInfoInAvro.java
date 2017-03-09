package dataformats.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;

 import dataformats.utils.AvroUtils;

/**
 * Demonstrating storage of avro schema using genericRecord class with avro schema.
 * 
 * @author Zak Hassan <zak.hassan@redhat.com>
 */
public class StoreCustomerInfoInAvro {
  private static final String CUSTOMER_AVRO = "customer.avro";
  Schema schema;

  public StoreCustomerInfoInAvro() throws IOException {
    schema =
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("customer.avsc"));
  }

  public void serialize() throws IOException {

    // Create new customers
    GenericRecord customer1 = createCustomer("Steve", 1);
    GenericRecord customer2 = createCustomer("Jeff", 2);
    GenericRecord customer3 = createCustomer("Justin", 3);
    GenericRecord customer4 = createCustomer("Harry", 4);
    GenericRecord customer5 = createCustomer("George", 5);

    // Serialize users
    DataFileWriter<GenericRecord> dataFileWriter =
        AvroUtils.createDataFileWriter(CUSTOMER_AVRO, schema);
    dataFileWriter.append(customer1);
    dataFileWriter.append(customer2);
    dataFileWriter.append(customer3);
    dataFileWriter.append(customer4);
    dataFileWriter.append(customer5);
    dataFileWriter.close();
    // Done!
    System.out.println("SUCCESS: Parsed Avro");

  }


  private GenericRecord createCustomer(String name, int id) {
    GenericRecord cust = new GenericData.Record(schema);
    cust.put("name", name);
    cust.put("id", id);
    return cust;
  }

  /*
  public void deserialize() throws IOException {
    DataFileReader<GeneratedCustomerBean> dataFR =
        AvroUtils.createDataFileReader(CUSTOMER_AVRO, GeneratedCustomerBean.class);
    GeneratedCustomerBean customer = null;
    while (dataFR.hasNext()) {
      customer = dataFR.next(customer);
      System.out.println(customer);
    }
    dataFR.close();
  }
*/
  public static void main(String[] args) throws IOException {
    StoreCustomerInfoInAvro poc = new StoreCustomerInfoInAvro();
    poc.serialize();
    //poc.deserialize();
  }

}
