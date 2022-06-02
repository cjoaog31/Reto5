/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package DBManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import jdk.nashorn.internal.parser.JSONParser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 * 
 */
public class DBManager 
{
    private static final String server = "localhost";
    private static final int port = 3306;
    private static final String database = "mysql";
    private static final String uid = "root";
    private static final String pwd = "Ju@n3lt0p0311";
    
    private static final String prodDB = "ferreteria";
    
    private Connection connection;
    private String connectionString;

    public DBManager()
    {
        this.connection = null;
        this.connectionString = "jdbc:mysql://" + server + ":" + port + "/" + database;
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
