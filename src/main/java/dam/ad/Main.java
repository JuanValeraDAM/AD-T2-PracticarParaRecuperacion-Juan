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

        Logger.getLogger("org.mongodb").setLevel(Level.WARNING);
        try (Connection oracle = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "practicarParaRecuperacion", "bla");
             Connection mysql = DriverManager.getConnection("jdbc:mysql://localhost:3306", "practicante", "bla");
             ClientSession basex = new ClientSession("localhost", 1984, "admin", "admin");
             MongoClient mongo = MongoClients.create()) {

            MongoCollection<Document> mongoCol = mongo.getDatabase("test").getCollection("personas");

            //Utiliza las operaciones de MongoDB para consultar la colección "empleados" y recuperar todos los empleados con un sueldo superior a 1000.

            /*    db.personas.find({sueldo:{$gt:1800}})
             */
            System.out.println("Consulta 1");
            List<Document> docs = mongoCol.find(Filters.gt("sueldo", 1800)).into(new ArrayList<>());
            docs.forEach(System.out::println);

            //Utiliza una operación de agregación en MongoDB para calcular el sueldo promedio de todos los empleados del departamento 2.
        /*
      db.personas.aggregate([
      {$match:{"departamento":2}},
      {$group:{_id:null, sueldoMedio:{$avg:"$sueldo"}}} ])
         */
            System.out.println("Consulta 2");
            Bson match = Aggregates.match(new Document("departamento", 2));
            Bson group = Aggregates.group("null", Accumulators.avg("SueldoMedio", "$sueldo"));
            List<Document> docss = mongoCol.aggregate(List.of(match, group)).into(new ArrayList<>());
            docss.forEach(System.out::println);

            //Consulta para obtener el promedio del sueldo de los empleados de cada departamento
            /*
 db.personas.aggregate([
 {$group:{_id:"$departamento", promedio:{$avg:"$sueldo"}}},
 {$project:{_id:false, departamento:"$_id", SueldoMedio:"$promedio"}}
 ])            */

            System.out.println("Consulta 3");
            Bson bsonGroup = Aggregates.group("$departamento", Accumulators.avg("promedio", "$sueldo"));
            Document etapaProject = new Document().append("_id", false).append("departamento", "$_id").append("SueldoMedio", "$promedio");
            Bson bsonProject = Aggregates.project(etapaProject);
            List<Document> docs3 = mongoCol.aggregate(List.of(bsonGroup, bsonProject)).into(new ArrayList<>());
            docs3.forEach(System.out::println);


            //Consulta para obtener el nombre y apellido del empleado con el sueldo más alto
            /*
            db.personas.aggregate([
			{$sort:{"sueldo":-1}},
			{$limit:1},
			{$project:{_id:false, Nombre:"$nombre", Apellidos:"$apellido"}}
			])
             */
            System.out.println("Consulta 4");
            Bson bsonSort=Aggregates.sort(new Document("sueldo", -1));
            Bson bsonLimit=Aggregates.limit(1);
            Document docProject = new Document().append("_id", false).append("Nombre", "$nombre").append("Apellidos", "$apellido");
            Bson bsonProject2=Aggregates.project(docProject);
            List<Document> docs4 =mongoCol.aggregate(List.of(bsonSort, bsonLimit,bsonProject2)).into(new ArrayList<>());
            docs4.forEach(System.out::println);


            //Consulta para obtener la cantidad de empleados en cada departamento
        /*
        db.personas.aggregate([
        {$group:{_id:"$departamento", total:{$sum:1}}},
        {$project:{_id:false, Departamento:"$_id", cantidadTotal:"$total"}}
        ])
         */
            //Consulta para obtener el nombre y apellido de los empleados que ganan más de $2000

        }
    }
}