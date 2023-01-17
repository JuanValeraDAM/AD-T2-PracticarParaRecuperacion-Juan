package dam.ad;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.basex.api.client.ClientSession;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {


    public static void main(String[] args) throws SQLException, IOException {


//Supuesto inventado con la colección personas:
//1.Crea una aplicación en Java que se conecte a MongoDB, BaseX, Oracle y MySQL.


        try (Connection oracle = DriverManager.getConnection("jdbc:oracle:thin:@localhost:49161:XE", "practicarParaRecuperacion", "bla");
             Connection mysql = DriverManager.getConnection("jdbc:mysql://localhost:3306", "practicante", "bla");
             ClientSession basex = new ClientSession("localhost", 1984, "admin", "admin");
             MongoClient mongo = MongoClients.create()) {

            MongoCollection<Document> mongoCol = mongo.getDatabase("test").getCollection("personas");
            Logger.getLogger("org.mongo").setLevel(Level.WARNING);
            //Utiliza las operaciones de MongoDB para consultar la colección "empleados" y recuperar todos los empleados con un sueldo superior a 1000.

            /*    db.personas.find({sueldo:{$gt:1000}})
             */
            List<Document> docs = mongoCol.find(Filters.gt("sueldo", 1000)).into(new ArrayList<>());
            docs.forEach(System.out::println);

//Utiliza una operación de agregación en MongoDB para calcular el sueldo promedio de todos los empleados del departamento 2.
        /*
      db.personas.aggregate([
      {$match:{"departamento":2}},
      {$group:{_id:null, sueldoMedio:{$avg:"$sueldo"}}} ])
         */

            Bson match = Aggregates.match(new Document("departamento", 2));
            Bson group = Aggregates.group("null", Accumulators.avg("SueldoMedio", "$sueldo"));
            List<Document> docss = mongoCol.aggregate(List.of(match, group)).into(new ArrayList<>());
            docss.forEach(System.out::println);

//Crea una conexión a una base de datos MySQL y crea una tabla llamada "empleados" con los mismos campos que los objetos JSON.
//Utiliza el array de objetos JSON para insertar registros en la tabla "empleados" de MySQL.
//Utiliza consultas SQL para seleccionar los empleados de MySQL con un sueldo superior a 2000 y mostrarlos en la consola.
//Utiliza una consulta SQL para calcular el promedio de sueldos de los empleados del departamento 3 en la tabla "empleados" de MySQL.
//Utiliza MongoDB para realizar una operación de actualización masiva para aumentar el sueldo de todos los empleados en un 10%.
//Utiliza la conexión de MySQL para actualizar los sueldos de los empleados en la tabla "empleados" con los cambios realizados en MongoDB.

        }
    }
}