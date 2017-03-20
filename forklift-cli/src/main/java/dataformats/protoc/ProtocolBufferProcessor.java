package dataformats.protoc;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufFactory;
import com.fasterxml.jackson.dataformat.protobuf.schema.NativeProtobufSchema;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema;
import com.fasterxml.jackson.dataformat.protobuf.schemagen.ProtobufSchemaGenerator;
import dataformats.model.CustomerBean;
import dataformats.utils.SchemaUtils;

public class ProtocolBufferProcessor {

  public static void main(String[] args) throws JsonMappingException {
    NativeProtobufSchema schema = SchemaUtils.toProtocolBuffer(CustomerBean.class);
    System.out.println(schema.toString());
  }

  
}
