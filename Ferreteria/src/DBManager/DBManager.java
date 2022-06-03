/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package DBManager;

import ferreteria.Ferreteria;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    private String initialConnectionString;
    private String connectionString;
    private static final String configFile = "config.xml";
    private Ferreteria ferreteria;
    
    private static final String TABLE_CREATION_FORMAT = "CREATE TABLE %s ( %s );";
    private static final String CREATE_DATABASE_FORMAT = "CREATE DATABASE %s";
    private static final String CONSTRAINT_FOREIGN_CREATION_FORMAT = "ALTER TABLE %s ADD CONSTRAINT FOREIGN KEY(%s) REFERENCES %s(%s)";
    private static final String INSERT_FORMAT = "INSERT INTO %s(%s) VALUES(?)";
    private static final String AUTO_INCREMENT = "auto_increment";
    private static final String PRIMARY_KEY = "primary key";

    /**
     * Constructor del DB Manager
     * Se encarga de crear la conexion a la base de datos leyendo el archivo de configuracion, en caso de que la base de datos inicial no exista crea la base de datos y cada una de las tablas necesarias 
     * estipuladas en el archivo de configuracion
     * @throws Exception 
     * 
     */
    public DBManager(Ferreteria ferreteria) throws Exception
    {
        this.ferreteria = ferreteria;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        
        try {
            
            System.out.println("Vamos a leer los valores necesarios para la conexion desde el XML");
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
            
            System.out.println("Obtuvo el nodo config");
            
            //Nombre del nodo que contiene la informacion de la base de datos
            NodeList atributosDB = doc.getElementsByTagName("DBInitialConnection");
            int tamResultado = atributosDB.getLength();
            if (tamResultado == 0) {
                throw new Exception("No se encontró el segmento de configuracion de la base de datos");
            }
            
            //Se espera que solo exista un item con este nombre, en caso de existir más de uno solo se procesa el primero
            Node configuracion = atributosDB.item(0);
            
            System.out.println("Se obtiene el primer elemento de configuracion de la base de datos");
            
            
            if (configuracion.getNodeType() == Node.ELEMENT_NODE) {

                  Element element = (Element) configuracion;

                  // Se obtienen los atributos basicos de la base de datos
                  String server = element.getElementsByTagName("server").item(0).getTextContent();
                  String motor = element.getElementsByTagName("motor").item(0).getTextContent();
                  String port = element.getElementsByTagName("port").item(0).getTextContent();
                  String database = element.getElementsByTagName("database").item(0).getTextContent();
                  String uid = element.getElementsByTagName("uid").item(0).getTextContent();
                  String pwd = element.getElementsByTagName("pwd").item(0).getTextContent();
                  
                  System.out.println("Se obtienen los valores de conexion a DB desde el XML");
                  
                  //Se inicializan los atributos de la clase
                  this.server = server;
                  this.motor = motor;
                  this.port = port;
                  this.database = database;
                  this.uid = uid;
                  this.pwd = pwd;
                  
                  System.out.println("Se inicializan los valores de conexion a DB");
            }
            
            NodeList atributosDBProd = doc.getElementsByTagName("DBProd");
            int tamResultado2 = atributosDBProd.getLength();
            if (tamResultado2 == 0) {
                throw new Exception("No se encontró el segmento de configuracion de la base de datos productiva");
            }
            
            System.out.println("Se obtiene el segmento del DB productiva para poder obtener el nombre");
            
            //Se espera que solo exista un item con este nombre, en caso de existir más de uno solo se procesa el primero
            Node nodoBaseDeDatos = atributosDBProd.item(0);
            
            if (nodoBaseDeDatos.getNodeType() == Node.ELEMENT_NODE) {

                  Element element = (Element) nodoBaseDeDatos;

                  // Se obtiene el nombre de la base de datos productiva
                  String prodDB = element.getElementsByTagName("database").item(0).getTextContent();
                  //Se inicializa el atributo
                  
                  System.out.println("Se obtiene el nombre de la base de datos productiva");
                  
                  this.prodDB = prodDB;
                  
                  System.out.println("Se termina de inicializar las variables de clase");
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
            this.initialConnectionString = "jdbc:mysql://" + server + ":" + port + "/" + database;
            this.connectionString = "jdbc:mysql://" + server + ":" + port + "/" + prodDB;
            System.out.println("Se crea el string de conexion");
        }
        else
        {
            throw new Exception("Por el momento no se soporta un motor distinto a mysql");
        }
        
        System.out.println("Se lanza el proceso de inicializacion de conexion con la base de datos");
        inicializar();
    }
    
    private void inicializar()
    {
        try 
        {
            if(this.connection == null)
            {
                System.out.println("La conexion no existe por lo tanto se va a crear");
                
                this.connection = DriverManager.getConnection(this.initialConnectionString, uid, pwd);
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
                        //TODO traer la informacion de la base de datos y cargar el modelo
                        cargarInformacion();
                    }
                    else
                    {
                        this.connection.close();
                        System.out.println("va a crear por primera vez");
                        inicializarDBPrimeraVez();
                    }
                }
                else
                {
                    throw new Exception("Error inicializando la conexion");
                }
            }
            
            
        } 
        catch (Exception e) 
        {
            System.out.println(e);
        }
    }
    
    private void cargarInformacion()
    {
        
    }
    
    private void inicializarDBPrimeraVez()
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        
        try {
            
            System.out.println("Comienza a leer el XML");
            // Se encarga de leer el archivo XML
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new File(System.getProperty("user.dir")+"/src/DBManager/"+configFile));
            
            doc.getDocumentElement().normalize();
            
          
            //Nombre del nodo que contiene la informacion de la base de datos productiva
            NodeList atributosDB = doc.getElementsByTagName("DBProd");
            //Se espera que solo exista un item con este nombre, en caso de existir más de uno solo se procesa el primero
            Node configuracion = atributosDB.item(0);
            
            System.out.println("Obtuvo el nodo de la base de datos productiva");
            
            if (configuracion.getNodeType() == Node.ELEMENT_NODE) {

                  Element element = (Element) configuracion;

                  // Se obtiene el nombre que se quiere usar para la base de datos
                   NodeList tablas = element.getElementsByTagName("tables");
                   int tamanioTablas = tablas.getLength();
                   
                   System.out.println("Esta obteniendo los nodos de tablas que deberia ser uno, es:" + tamanioTablas);
                   
                   if (tamanioTablas == 0)
                   {
                       throw new Exception("El archivo de configuracion no cuenta con la estructura de tablas necesaria");
                   }
                   
                   //Solamente va a tomar el primer listado de tablas si hay más no lo toma
                   Element tablasElement = (Element) tablas.item(0);
                   NodeList tablasList = tablasElement.getElementsByTagName("table");
                   int cantidadTablas = tablasList.getLength();
                   
                   if (cantidadTablas == 0)
                   {
                       throw new Exception("El archivo de configuracion no cuenta con la estructura de tablas necesaria");
                   }
                                      
                   //Arreglo donde se guardan los constraints
                   ArrayList<String> constraints = new ArrayList<String>();
                   //Arreglo donde se guardan las definiciones de las tablas
                   ArrayList<String> tablesString = new ArrayList<String>();
                   //Arreglo donde se guardan los valores predeterminados
                   ArrayList<HashMap> valoresPredeterminados = new ArrayList<HashMap>();
                   
                   System.out.println("Vamos a iterar entre los nodos tabla");
                   
                   for (int i = 0; i < cantidadTablas; i++) 
                   {
                       //Detalles de las columnas
                       ArrayList<String> columnasArray = new ArrayList<String>();
                       
                       Element tabla = (Element) tablasList.item(i);
                       
                       //Nombre de la tabla que se está evaluando
                       String nombreTabla = tabla.getElementsByTagName("name").item(0).getTextContent();
                       
                       System.out.println("Se obtuvo la tabla: " + nombreTabla);
                       
                       //Lista de columnas
                       NodeList columnas = tabla.getElementsByTagName("columna");
                       int tamanioColumnas = columnas.getLength();
                       if (tamanioTablas == 0)
                       {
                           throw new Exception("La tabla: " + nombreTabla + " no tiene columnas en el archivo de configuracion");
                       }
                       
                       System.out.println("Se listaron las columnas de la tabla " + nombreTabla + " " + tamanioColumnas);
                       
                       //Obtiene la informacion de las columnas
                       for (int j = 0; j < tamanioColumnas; j++) 
                       {
                           
                           Element columna = (Element) columnas.item(j);
                           
                           String nombre = columna.getElementsByTagName("nombre").item(0).getTextContent();
                           String tipoDato = columna.getElementsByTagName("tipoDato").item(0).getTextContent();
                           String isNull = columna.getElementsByTagName("null").item(0).getTextContent();
                           if (isNull.equalsIgnoreCase("false"))
                           {
                               isNull = "not null";
                           }
                           else if (isNull.equalsIgnoreCase("true"))
                           {
                               isNull = "null";
                           }
                           //System.out.println("Obtiene los valores basicos de la columna");
                           
                           //Almacena los datos de la columna
                           String valoresColumna = nombre + " " + tipoDato + " " + isNull;
                           
                           //Si cuenta con un nodo primary asume que la columna es una primary key
                           NodeList primaryList = columna.getElementsByTagName("primary");
                           if (primaryList.getLength() > 0) 
                           {
                               valoresColumna += " " + PRIMARY_KEY;                               
                           }
                           
                           //Si cuenta con un nodo auto_increment asume que la columna es auto incrementable
                           NodeList autoIncrementList = columna.getElementsByTagName("auto_increment");
                           if (autoIncrementList.getLength() > 0) 
                           {
                               valoresColumna += " " + AUTO_INCREMENT;
                           }
                           
                           
                           //Si cuenta con un nodo auto_increment asume que la columna es auto incrementable
                           NodeList foreignList = columna.getElementsByTagName("foreign");
                           if (foreignList.getLength() > 0) 
                           {
                               String referencedColumn = foreignList.item(0).getAttributes().getNamedItem("referencedColumn").getTextContent();
                               String referencedTable = foreignList.item(0).getAttributes().getNamedItem("referencedTable").getTextContent();
                               String foreignKeyData = nombreTabla + "," + nombre + "," + referencedTable + "," + referencedColumn;
                               constraints.add(foreignKeyData);
                           }
                           
                           System.out.println("Obtiene los valores opcionales de la columna");
                           
                           //Adiciona el valor de la columna a la lista de columnas
                           columnasArray.add(valoresColumna);
                           
                       }
                       
                       System.out.println("Termina de iterar las columnas");
                       
                       
                       
                       
                       System.out.println("Adiciona la definicion de la tabla");
                       
                       //Si cuenta con valores predeterminados
                        NodeList valoresList = tabla.getElementsByTagName("valores");
                        if (valoresList.getLength() > 0) 
                        {
                            System.out.println("La tabla " + nombreTabla + " cuenta con valores predeterminados");
                            Element valorElement = (Element) valoresList.item(0);
                            NodeList valorList = valorElement.getElementsByTagName("value");
                            int tamanioValor = valorList.getLength();
                            
                            if (tamanioValor > 0) 
                            {
                                for (int k = 0; k < tamanioValor; k++)
                                {
                                    Element valor = (Element) valorList.item(k);
                                    
                                    HashMap<String, String> valores = new HashMap<String, String>();
                                    valores.put("tabla", nombreTabla);
                                    
                                    for (String item : columnasArray)
                                    {
                                        String columna = item.split(",")[0];
                                        NodeList dato = valor.getElementsByTagName(columna);
                                        
                                        if (dato.getLength() > 0) 
                                        {
                                            String atributo = dato.item(0).getTextContent();
                                            valores.put(columna, atributo);
                                        }
                                    }
                                    
                                    //Adiciona el mapa con el valor predeterminado al listado
                                    valoresPredeterminados.add(valores);
                                }
                            }
                            System.out.println("Termina de crear los valores predeterminados");
                        }
                        //Crea el string que define a la tabla
                       String valorTabla = nombreTabla; 
                       int cantidadColumnas = columnasArray.size();
                       
                       for (int j = 0; j < cantidadColumnas; j++) 
                       {
                           String valorI = columnasArray.get(j);
                           valorTabla += "," + valorI;
                       }
                       tablesString.add(valorTabla);
                   }
                   
                   System.out.println("Vamos a comenzar a efectuar los cambios a nivel de base de datos");
                   
                   /**
                    * Segmento para inicializar la conexion 
                    */
                   
                   if(this.connection == null || this.connection.isClosed())
                   {
                       this.connection = DriverManager.getConnection(this.initialConnectionString, uid, pwd);
                       boolean valid = this.connection.isValid(50000);
                       
                       if (!valid) 
                       {
                           throw new Exception("Error creando la conexion");
                       }
                   }
                   
                   /**
                    * Segmento donde se crea la base de datos
                    */
                   
                   String createDatabase = String.format(CREATE_DATABASE_FORMAT, this.prodDB);
                   PreparedStatement databaseCreationStatement = this.connection.prepareStatement(createDatabase);
                   //System.out.println(databaseCreationStatement);
                   int rowsUpdated = databaseCreationStatement.executeUpdate();
                   if (rowsUpdated == 0) 
                   {
                       throw new Exception("No se pudo crear la base de datos");
                   }
                   
                   System.out.println("Creo la base de datos inicia con las tablas");
                   this.connection.close();
                   
                   
                   /**
                    * Segmento para inicializar la conexion 
                    */
                   
                   if(this.connection == null || this.connection.isClosed())
                   {
                       this.connection = DriverManager.getConnection(this.connectionString, uid, pwd);
                       boolean valid = this.connection.isValid(50000);
                       
                       if (!valid) 
                       {
                           throw new Exception("Error creando la conexion");
                       }
                   }
                   
                   /**
                    * Segmento donde se crean las tablas
                    */
                   
                   for (String stringDefinicionTabla : tablesString) 
                   {
                       
                       //Cambia el nombre de la tabla
                       String[] elementos = stringDefinicionTabla.split(",");
                       int cantidadColumnas = elementos.length;

                       String definicionColumnas = "";
                       
                       for (int i = 1; i < cantidadColumnas; i++) 
                       {
                           if (i == cantidadColumnas-1) 
                           {
                               definicionColumnas += elementos[i];
                           }
                           else
                           {
                               definicionColumnas += elementos[i] + ", ";
                           }
                       }
                       
                       String[] argumentos = new String[2];
                       
                       argumentos[0]= elementos[0];
                       argumentos[1]= definicionColumnas;
                       
                       String creacionDeTabla = String.format(TABLE_CREATION_FORMAT, argumentos);
                       System.out.println(creacionDeTabla);
                       PreparedStatement tableCreationStatement = this.connection.prepareStatement(creacionDeTabla);
                       int rowsUpdated1 = tableCreationStatement.executeUpdate();
                       if (rowsUpdated == 0) 
                       {
                            throw new Exception("No se pudo crear la tabla: " + elementos[0]);
                       }
                   }
                   
                   /**
                    * Segmento donde se crean las tablas
                    */
                   
                   
            }
            else
            {
                throw new Exception("No se sabe que error se presentó con la definicion de la configuracion");
            }
        } 
        catch (Exception e) 
        {
            System.out.println(e);
        }
    }

    public void actualizarNombreFerreteria(String nuevoNombre) 
    {
        String sql = "update config set nombre = ?";
        try
        {
            PreparedStatement statement = this.connection.prepareStatement(sql);
            statement.setString(1, nuevoNombre);
            int rowsUpdated = statement.executeUpdate();
            if (rowsUpdated > 0 )
            {
                System.out.println("Se actualizó el nombre de la ferreteria");
            }
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(DBManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    
    
    
}
