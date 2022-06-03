/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package ferreteria;

import DBManager.DBManager;

/**
 *
 * @author ragnarokdx
 */
public class Ferreteria {

    /**
     * @param args the command line arguments
     */
    
    private String nombre;
    private DBManager dbmanager;

    public Ferreteria() 
    {
        this.nombre = "";
        try 
        {
            System.out.println("Creando la DBManager");
            this.dbmanager = new DBManager(this);
        } 
        catch (Exception e) 
        {
            System.out.println("Error inicializando la base de datos");
        }
        
    }
    
    public void cambiarNombreFerreteria(String nuevoNombre)
    {
        this.nombre = nuevoNombre;
        dbmanager.actualizarNombreFerreteria(nuevoNombre);
    }
    
    public static void main(String[] args) 
    {
        Ferreteria ferr = new Ferreteria();
    }

    public void cargarInformacionFerreteria() 
    {
        String nombre = "";
        String direccion = "";
        
        this.nombre = nombre;
    }
    
}
