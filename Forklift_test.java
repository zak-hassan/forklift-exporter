/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package forklift_test;

import com.google.gson.Gson;
import java.io.*;
import java.util.ArrayList;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;





/**  
 *
  * @author ykassim
 */
public class Forklift_test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        flift test = testobject();
        Gson gson = new Gson();
        String json = gson.toJson(test);
        //System.out.println(json);
        //System.out.println(args[0]);
        
       try (FileWriter filew = new FileWriter("/home/ykassim/json_test")){
           
           gson.toJson(test,filew);
       } catch (IOException e){
           e.printStackTrace();
       }
       
       String server =  args[0];//"ftp.dlptest.com";
       int port = 21;
       String user = args[1];//"dlpuser@dlptest.com";
       String pass = args[2];//"fwRhzAnR1vgig8s";
       
       FTPClient ftpClient = new FTPClient();
              
       try {
           
           ftpClient.connect(server,port);
           ftpClient.login(user,pass);
           ftpClient.enterLocalPassiveMode();
           
           ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
           
           File localfile = new File("/home/ykassim/json_test");
           
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
        
    private static flift testobject(){
        
            
            flift test = new flift();
            test.getlist();
            
            return test;
    }
        
        // TODO code application logic here
    
}

