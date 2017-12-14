package com.my.axa;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import com.opencsv.CSVWriter;

import java.util.Date;

public class ReadAS400 {

	// Create one directory
	private static String createFolder(String strPath)
	{
		String AbsolutePath = strPath;
        
        DateFormat dateFormat = new SimpleDateFormat("dd_MM_yyyy");
        Date date = new Date();
        String toDate = dateFormat.format(date).toString(); //2016/11/16 12:08:43
        String csvFilePath = AbsolutePath+"\\" + toDate;
        		
	    boolean success = (new File(csvFilePath)).mkdir();
	    if (success) 
	    {
	      System.out.println("Directory: " + csvFilePath + " created");
	    }
	    else
	    {
	    	System.out.println("Failed to create directory: " + csvFilePath + ". Please check folder already exist.");
	    	System.exit(1);
	    }
	    return csvFilePath;
	}

	// Compress the give folder location
	public static void pack(String sourceDirPath, String zipFilePath) throws IOException {
	    Path p = Files.createFile(Paths.get(zipFilePath));
	    try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(p))) {
	        Path pp = Paths.get(sourceDirPath);
	        Files.walk(pp)
	          .filter(path -> !Files.isDirectory(path))
	          .forEach(path -> {
	              ZipEntry zipEntry = new ZipEntry(pp.relativize(path).toString());
	              try {
	                  zs.putNextEntry(zipEntry);
	                  zs.write(Files.readAllBytes(path));
	                  zs.closeEntry();
	            } catch (Exception e) {
	                System.err.println(e);
	                System.exit(1);
	            }
	          });
	    }
	}
	
	public static void main(String[] args) 
	{
        String strPath			= args[1];
        String xmlFileName      = strPath + "\\" + args[0];
        String system           = "";
        String userName 		= "";
        String userPswd			= "";
        Boolean includeHeaders 	= true;
        String s_csvfileName 	= "";
        String s_database		= "";
        String s_tableName		= "";
        String s_columns		= "";

        String propFileName 	= strPath + "\\as400.properties";
        InputStream  inStream 	= null;
        
        // Creating folder for todays date
        String csvFilePath = createFolder(strPath);
        
        // Reading properties file.
        try 
        {
            Properties prop = new Properties();
    		inStream = new FileInputStream(propFileName);

        	// load a properties file
    		prop.load(inStream);

    		// get the property value and print it out
    		system 		= prop.getProperty("system");
    		userName 	= prop.getProperty("username");
    		userPswd	= prop.getProperty("userpassword");
    	} 
        catch (IOException io) 
        {
    		io.printStackTrace();
    		System.exit(1);
    	}
        
        
    	// Checking driver class.
        try
        {
        	Class.forName("com.ibm.as400.access.AS400JDBCDriver");
        }
        catch(ClassNotFoundException e)
        {
        	System.out.println(e);
        	System.exit(1);
        }
        
        Connection connection   = null;

        // Reading table from AS400 and storing into a CSV format
        try {

            // Load the IBM Toolbox for Java JDBC driver.
            DriverManager.registerDriver(new com.ibm.as400.access.AS400JDBCDriver());

            // Get a connection to the database.
            connection = DriverManager.getConnection ("jdbc:as400://" + system + "" , userName, userPswd);
            System.out.println("AS400 Sucessfully Connected...");
            
            // Reading extract_table.xml file for extracting table from AS400
            File fXmlFile = new File(xmlFileName);
        	DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        	DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        	Document doc = dBuilder.parse(fXmlFile);
        	doc.getDocumentElement().normalize();

         	NodeList nList = doc.getElementsByTagName("table");
        	System.out.println("No of table extraction: " + nList.getLength());
        	
        	for (int temp = 0; temp < nList.getLength(); temp++) 
        	{
        		Node nNode = nList.item(temp);

        		if (nNode.getNodeType() == Node.ELEMENT_NODE) 
        		{
        			Element eElement = (Element) nNode;
        			System.out.println("Table id : " + eElement.getAttribute("id"));
        			s_columns 		= eElement.getElementsByTagName("columns").item(0).getTextContent();
        			s_database 		= eElement.getElementsByTagName("database").item(0).getTextContent();
        			s_tableName 	= eElement.getElementsByTagName("tablename").item(0).getTextContent();
        			s_csvfileName	= csvFilePath + "\\" + eElement.getElementsByTagName("csvfilename").item(0).getTextContent();
        		}

        		// Execute the query.
	            System.out.println("Reading table : " + s_tableName);
	            Statement select = connection.createStatement ();
	            ResultSet rs = select.executeQuery ("SELECT " + s_columns + " FROM " + s_database + "." + s_tableName);
	            
	            System.out.println("Writing table into a CSV format : " + s_csvfileName);
	            CSVWriter writer = new CSVWriter(new FileWriter(s_csvfileName));
	
	            writer.writeAll(rs, includeHeaders);
	            writer.close();
	            
	            System.out.println ("Sucessfully File Created...");
        	}
        	
        	System.out.println ("Sucessfully All Table extracted...");
        	
        	// Compress the folder
        	/*String zipFileName=csvFilePath + ".zip";
        	pack(csvFilePath, zipFileName);
        	System.out.println ("Sucessfully Compress File Created...");
        	
        	// Move file
        	File afile =new File(zipFileName);
        	if(afile.renameTo(new File(ftppath + "\\" + afile.getName())))
        	{
        		System.out.println("File is moved successful!");
        	}else{
        		System.out.println("File is failed to move!");
        	}*/

       }
       catch (Exception e) 
       {
            System.out.println ();
            e.printStackTrace();
            System.out.println ("ERROR: " + e.getMessage());
            System.exit(1);
       }
       finally 
       {
            // Clean up.
            try {
                if (connection != null)
                    connection.close ();
            }
            catch (SQLException e) {
                // Ignore.
            }
        }

	}
}
