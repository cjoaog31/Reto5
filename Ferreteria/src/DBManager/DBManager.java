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
import java.util.ArrayList;
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
    private String port;
    private String database;
    private String uid;
    private String pwd;
    private String motor;
    private String prodDB;
    
    private Connection connection;
    private String connectionString;
    private static final String configFile = "config.xml";
    
    private static final String TABLE_CREATION_FORMAT = "CREATE TABLE ? ( ? );";
    private static final String CONSTRAINT_CREATION_FORMAT = "";
    private static final String AUTO_INCREMENT = "auto_increment";
    private static final String PRIMARY_KEY = "primary key";

    /**
     * Constructor del DB Manager
     * Se encarga de crear la conexion a la base de datos leyendo el archivo de configuracion, en caso de que la base de datos inicial no exista crea la base de datos y cada una de las tablas necesarias 
     * estipuladas en el archivo de configuracion
     * @throws Exception 
     * 
     */
    public DBManager() throws Exception
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        
        try {
            
            // Se encarga de leer el archivo XML
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new File(System.getProperty("user.dir")+"/src/DBManager/"+configFile));
            
            doc.getDocumentElement().normalize();
            
            if(!doc.getDocumentElement().getNodeName().equals("config"))
            {
                //Arroja exception en caso de que el archivo no tenga un segmento config el cual es parte de la estructura base
                throw new Exception("El archivo de configuracion no esta bien estructurado");
            }
            
            //Nombre del nodo que contiene la informacion de la base de datos
            NodeList atributosDB = doc.getElementsByTagName("DBInitialConnection");
            int tamResultado = atributosDB.getLength();
            if (tamResultado == 0) {
                throw new Exception("No se encontró el segmento de configuracion de la base de datos");
            }
            
            //Se espera que solo exista un item con este nombre, en caso de existir más de uno solo se procesa el primero
            Node configuracion = atributosDB.item(0);
            
            if (configuracion.getNodeType() == Node.ELEMENT_NODE) {

                  Element element = (Element) configuracion;

                  // Se obtienen los atributos basicos de la base de datos
                  String server = element.getElementsByTagName("server").item(0).getTextContent();
                  String motor = element.getElementsByTagName("motor").item(0).getTextContent();
                  String port = element.getElementsByTagName("port").item(0).getTextContent();
                  String database = element.getElementsByTagName("database").item(0).getTextContent();
                  String uid = element.getElementsByTagName("uid").item(0).getTextContent();
                  String pwd = element.getElementsByTagName("pwd").item(0).getTextContent();
                  
                  //Se inicializan los atributos de la clase
                  this.server = server;
                  this.motor = motor;
                  this.port = port;
                  this.database = database;
                  this.uid = uid;
                  this.pwd = pwd;
            }
            
            NodeList atributosDBProd = doc.getElementsByTagName("DBProd");
            int tamResultado2 = atributosDBProd.getLength();
            if (tamResultado2 == 0) {
                throw new Exception("No se encontró el segmento de configuracion de la base de datos productiva");
            }
            
            //Se espera que solo exista un item con este nombre, en caso de existir más de uno solo se procesa el primero
            Node nodoBaseDeDatos = atributosDBProd.item(0);
            
            if (nodoBaseDeDatos.getNodeType() == Node.ELEMENT_NODE) {

                  Element element = (Element) nodoBaseDeDatos;

                  // Se obtiene el nombre de la base de datos productiva
                  String prodDB = element.getElementsByTagName("database").item(0).getTextContent();
                  //Se inicializa el atributo
                  this.prodDB = prodDB;

            }
            
        } 
        catch (Exception e) 
        {
            System.out.println(e);
        }
        
        //Se inicializa la conexion nula mientras se hace la validacion
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
    
    private void inicializar()
    {
        try 
        {
            this.connection = DriverManager.getConnection(this.connectionString, uid, pwd);
            boolean valid = this.connection.isValid(50000);
            System.out.println(valid);
            if (valid)
            {
                String DBExists = "show databases like '" + prodDB + "';";
                System.out.println(DBExists);
                Statement DB = this.connection.createStatement();
                ResultSet executed = DB.executeQuery(DBExists);
                if (executed.next())
                {
                    //TODO
                }
                else
                {
                    inicializarDBPrimeraVez();
                }
            }
            else
            {
                throw new Exception("Error inicializando la conexion");
            }
            
        } 
        catch (Exception e) 
        {
            System.out.println(e);
        }
    }
    
    private void inicializarDBPrimeraVez()
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        
        try {
            
            // Se encarga de leer el archivo XML
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new File(System.getProperty("user.dir")+"/src/DBManager/"+configFile));
            
            doc.getDocumentElement().normalize();
            
          
            //Nombre del nodo que contiene la informacion de la base de datos productiva
            NodeList atributosDB = doc.getElementsByTagName("DBProd");
            //Se espera que solo exista un item con este nombre, en caso de existir más de uno solo se procesa el primero
            Node configuracion = atributosDB.item(0);
            
            if (configuracion.getNodeType() == Node.ELEMENT_NODE) {

                  Element element = (Element) configuracion;

                  // Se obtiene el nombre que se quiere usar para la base de datos
                   NodeList tablas = element.getElementsByTagName("tables");
                   int tamanioTablas = tablas.getLength();
                   
                   if (tamanioTablas == 0)
                   {
                       throw new Exception("El archivo de configuracion no cuenta con la estructura de tablas necesaria");
                   }
                   
                   //Arreglo donde se guardan los constraints
                   ArrayList<String> constraints = new ArrayList<String>();
                   
                   for (int i = 0; i < tamanioTablas; i++) 
                   {
                       Element tabla = (Element) tablas.item(i);
                       
                       //Nombre de la tabla que se está evaluando
                       String nombreTabla = tabla.getElementsByTagName("name").item(0).getTextContent();
                       
                       //Lista de columnas
                       NodeList columnas = tabla.getElementsByTagName(port);
                       int tamanioColumnas = columnas.getLength();
                       if (tamanioTablas == 0)
                       {
                           throw new Exception("La tabla: " + nombreTabla + " no tiene columnas en el archivo de configuracion");
                       }
                       
                       for (int j = 0; j < tamanioColumnas; j++) 
                       {
                           Element columna = (Element) columnas.item(0);
                           
                           String nombre = columna.getElementsByTagName("nombre").item(0).getTextContent();
                           String tipoDato = columna.getElementsByTagName("tipDato").item(0).getTextContent();
                           String isNull = columna.getElementsByTagName("null").item(0).getTextContent();
                           boolean primary = false;
                           boolean foreign = false;
                           boolean autoIncrement = false;
                           
                           NodeList primaryList = null;
                       }
                   }
                  
                  System.out.println("DBManager.DBManager.inicializarDBPrimeraVez()");
            }
        } 
        catch (Exception e) 
        {
            System.out.println(e);
        }
    }
    
    
    
    
}
