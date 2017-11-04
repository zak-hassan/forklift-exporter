/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forklift_test;

import com.google.gson.Gson;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema; //import module
import org.apache.avro.generic.GenericRecord; //import module




/**  
 *
  * @author ykassim
 */
public class Forklift_test {
    
    
    public class FTPsink implements flift {
        
        public List serialize(String file) {
            
            try {
                Scanner s = new Scanner(new File(file));
                ArrayList<String> orders = new ArrayList<String>();
                while (s.hasNext()){
                    orders.add(s.next());
                }
                s.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Forklift_test.class.getName()).log(Level.SEVERE, null, ex);
            }
            
                     
            
            Schema schema = SchemaUtils.toAvroSchema(Order.class);
            List<GenericRecord> list = Converter.getGenericRecords(orders, schema);
            return list;
            
            
        }
        
        
      
            
        
    }
    

    /**
     * @param args the command line arguments
     */
    public void main(String[] args) throws FileNotFoundException {
        
        
       flift example = new FTPsink();
       List my_obj = example.serialize("/home/ykassim/json_test");    
       
       
       PrintWriter pw = new PrintWriter(new File("/home/ykassim/json-1"));
       int i = 0;
       pw.print(my_obj.get(i));
       
       /*
       try (FileWriter filew = new FileWriter("/home/ykassim/json-1")){
           
           //gson.toJson(test,filew);
       } catch (IOException e){
           e.printStackTrace();
       }
       
       */
       
       String server = "ftp.dlptest.com";
       int port = 21;
       String user = "dlpuser@dlptest.com";
       String pass = "fwRhzAnR1vgig8s";
       
       FTPClient ftpClient = new FTPClient();
              
       try {
           
           ftpClient.connect(server,port);
           ftpClient.login(user,pass);
           ftpClient.enterLocalPassiveMode();
           
           ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
           
           File localfile = new File("/home/ykassim/json-1");
           
           String remotefile = "upload2";
           InputStream inputstream = new FileInputStream(localfile);
           
           System.out.println("Start uploading first file");
           boolean done = ftpClient.storeFile(remotefile, inputstream);
           inputstream.close();
           if (done) {
               System.out.println("Upload Successful!");
               //ftpClient.logout();
               //ftpClient.disconnect();
               
               
           }
           
                
           
           
           
           
       } catch (IOException ex) {
           System.out.println("Error: " + ex.getMessage());
           ex.printStackTrace();
           
       } finally {
           
       
           try{
               if (ftpClient.isConnected()) {
                   ftpClient.logout();
                   ftpClient.disconnect();
               }
                   
               } catch (IOException ex) {
                   ex.printStackTrace();
               }
           }
          
        
        
        
    }
    
     
}

