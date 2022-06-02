/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package DBManager;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * 
 */
public class DBManager 
{
    private String server;
    private int port;
    private String database;
    private String uid;
    private String pwd;
    private String motor;
    private String prodDB;
    
    private Connection connection;
    private String connectionString;
    private static final String configFile = "config.xml";

    public DBManager() throws Exception
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        
        try {
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new File(System.getProperty("user.dir")+"/src/DBManager/"+configFile));
            
            doc.getDocumentElement().normalize();
            
            if(!doc.getDocumentElement().getNodeName().equals("config"))
            {
                throw new Exception("El archivo de configuracion no esta bien estructurado");
            }
            
            NodeList atributosDB = doc.getElementsByTagName("DBInitialConnection");
            int tamResultado = atributosDB.getLength();
            if (tamResultado == 0) {
                throw new Exception("No se encontró el segmento de configuracion de la base de datos");
            }
            
            Node configuracion = atributosDB.item(0);
            
            if (configuracion.getNodeType() == Node.ELEMENT_NODE) {

                  Element element = (Element) configuracion;

                  // get staff's attribute
                  String server = element.getElementsByTagName("server").item(0).getTextContent();
                  String motor = element.getElementsByTagName("motor").item(0).getTextContent();
                  String port = element.getElementsByTagName("port").item(0).getTextContent();
                  String database = element.getElementsByTagName("database").item(0).getTextContent();
                  String uid = element.getElementsByTagName("uid").item(0).getTextContent();
                  String pwd = element.getElementsByTagName("pwd").item(0).getTextContent();
                  
                  this.server = server;
                  this.motor = motor;
                  this.port = Integer.getInteger(port);
                  this.database = database;
                  this.uid = uid;
                  this.pwd = pwd;
              }
        } 
        catch (Exception e) 
        {
            System.out.println(e);
        }
        this.connection = null;
        if(motor.equalsIgnoreCase("mysql"))
        {
            this.connectionString = "jdbc:mysql://" + server + ":" + port + "/" + database;
        }
        else
        {
            throw new Exception("Por el momento no se soporta un motor distinto a mysql");
        }
        inicializar();
    }
    
    public void inicializar()
    {
        try 
        {
            this.connection = DriverManager.getConnection(this.connectionString, uid, pwd);
            boolean valid = this.connection.isValid(50000);
            
            if (valid)
            {
                String DBExists = "show databases like '" + prodDB + "';";
                Statement DB = this.connection.createStatement();
                ResultSet executed = DB.executeQuery(DBExists);
                if (executed.next())
                {
                    String dbExistance = executed.getString(1);
                    if (!dbExistance.equalsIgnoreCase(prodDB)) 
                    {
                        inicializarEstructura();
                    }
                }
            }
            else
            {
                throw new Exception("Error inicializando la conexion");
            }
            
        } 
        catch (Exception e) 
        {
            System.out.println(e.getLocalizedMessage());
        }
    }
    
    private void inicializarEstructura()
    {
        
    }
    
    
    
}
