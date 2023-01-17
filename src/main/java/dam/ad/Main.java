package dam.ad;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
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
        try (Connection oracle = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE", "practicarrecuperacionAD", "bla");
             Connection mysql = DriverManager.getConnection("jdbc:mysql://localhost:3306", "practicante", "bla");
             ClientSession basex = new ClientSession("localhost", 1984, "admin", "admin");
             MongoClient mongo = MongoClients.create()) {

            MongoCollection<Document> mongoCol = mongo.getDatabase("test").getCollection("personas");

            //Utiliza las operaciones de MongoDB para consultar la colección "empleados" y recuperar todos los empleados con un sueldo superior a 1900.

            /*    db.personas.find({sueldo:{$gt:1900}})
             */
            System.out.println("Consulta 1");
            List<Document> docs = mongoCol.find(Filters.gt("sueldo", 1900)).into(new ArrayList<>());
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
            Bson bsonSort = Aggregates.sort(new Document("sueldo", -1));
            Bson bsonLimit = Aggregates.limit(1);
            Document docProject = new Document().append("_id", false).append("Nombre", "$nombre").append("Apellidos", "$apellido");
            Bson bsonProject2 = Aggregates.project(docProject);
            List<Document> docs4 = mongoCol.aggregate(List.of(bsonSort, bsonLimit, bsonProject2)).into(new ArrayList<>());
            docs4.forEach(System.out::println);


            //Consulta para obtener la cantidad de empleados en cada departamento
        /*
        db.personas.aggregate([
        {$group:{_id:"$departamento", total:{$sum:1}}},
        {$project:{_id:false, Departamento:"$_id", cantidadTotal:"$total"}}
        ])
         */
            System.out.println("Consulta 5");
            Bson bsonGroup2 = Aggregates.group("$departamento", Accumulators.sum("total", 1));
            Bson bsonProject3 = Aggregates.project(new Document("_id", false)
                    .append("Departamento", "$_id").append("Cantidad", "$total"));
            List<Document> docs5 = mongoCol.aggregate(List.of(bsonGroup2, bsonProject3)).into(new ArrayList<>());
            docs5.forEach(System.out::println);

            //Consulta para obtener el nombre y apellido de los empleados que ganan más de $1500
        /*
        db.personas.aggregate([
        {$match:{"sueldo":{$gt:1900}}},
        {$project:{_id:false, Nombre:"$nombre", Apellidos:"$apellido"}}
        ])
         */
            System.out.println("Consulta 6");
            Bson bsonMatch = Aggregates.match(Filters.gt("sueldo", 1900));
            Bson bsonProject6 = Aggregates.project(new Document("_id", false).append("Nombre", "$nombre").append("Apellidos", "$apellido"));
            List<Document> docs6 = mongoCol.aggregate(List.of(bsonMatch, bsonProject6)).into(new ArrayList<>());
            docs6.forEach(System.out::println);


            //Obtener los 2 departamentos con mayor sueldo
        /*
        db.personas.aggregate([
        {$group:{_id:"$departamento", Sueldos:{$sum:"$sueldo"}}},
        {$sort:{"Sueldos":-1}},
        {$limit:2}
        ])
         */
            System.out.println("Consulta 7");
            Bson bsonGroup7 = Aggregates.group("$departamento", Accumulators.sum("Sueldos", "$sueldo"));
            Bson bsonSort7 = Aggregates.sort(new Document("Sueldos", -1));
            Bson bsonLimit7 = Aggregates.limit(2);

            List<Document> docs7 = mongoCol.aggregate(List.of(bsonGroup7, bsonSort7, bsonLimit7)).into(new ArrayList<>());
            docs7.forEach(System.out::println);

            //Otra colección
            MongoCollection<Document> coleccion2 = mongo.getDatabase("test").getCollection("skynet");
            // 1. Nombre y multas de las personas que tengan alguna multa de 100 y alguna de 200.
            /*
            db.skynet.aggregate([
                    {$match:{"multas":100}},
                    {$match:{"multas":200}},
                    {$project:{_id: false, Nombre:"$nombre", Multas:"$multas"}}
                    ])
             */
            System.out.println("Consulta 8");
            Bson bsonMatch8 = Aggregates.match(Filters.eq("multas", 100));
            Bson bsonMatch7 = Aggregates.match(Filters.eq("multas", 200));
            Bson bsonProject8 = Aggregates.project(new Document("Nombre", "$nombre").append("Multas", "$multas").append("_id", false));
            List<Document> docs8 = coleccion2.aggregate(List.of(bsonMatch8, bsonMatch7, bsonProject8)).into(new ArrayList<>());
            docs8.forEach(System.out::println);


            // 2. Cantidad de personas que tienen alguna multa mayor o igual a 200,
            // o son aficionadas a la programación, o son del país "Spain".
            /*
                db.skynet.aggregate([
                {$match:{$or:[
                {"multas":{$gte:200}},{"aficiones":"Programación"},{"pais":"Spain"}
                ]}},
                {$count:"Cantidad de personas:"}
                ])
             */
            System.out.println("Consulta 9");
            Bson bsonMatch9 = Aggregates.match(Filters.or(
                    Filters.gte("multas", 200),
                    new Document("aficiones", "Programación"),
                    new Document("pais", "Spain")));
            Bson bsonCount9 = Aggregates.count("Cantidad de personas");
            List<Document> docs9 = coleccion2.aggregate(List.of(bsonMatch9, bsonCount9)).into(new ArrayList<>());
            docs9.forEach(System.out::println);


            // 3. Cantidad de personas aficionadas al deporte por cada país, pero solamente para los países donde haya más de una.
            /*
            db.skynet.aggregate([
            {$match:{aficiones:"Deporte"}},
            {$group:{_id:"$pais", cantidad:{$sum:1}}},
            {$match:{cantidad:{$gt:1}}}
            ])
             */
            System.out.println("Consulta 10");
            Bson bsonMatch10 = Aggregates.match(new Document("aficiones", "Deporte"));
            Bson bsonGroup10 = Aggregates.group("$pais", Accumulators.sum("cantidad", 1));
            Bson bsonMatch101 = Aggregates.match(Filters.gt("cantidad", 1));

            List<Document> docs10 = coleccion2.aggregate(List.of(bsonMatch10, bsonGroup10, bsonMatch101)).into(new ArrayList<>());
            docs10.forEach(System.out::println);

            // 4. Añade la afición "Programación" a todas las personas que no la tengan ya, y muestra cuántas se han modificado.
            /*
            db.skynet.updateMany([
            {aficiones:{$ne:"Programación"}},
            {$addToSet:{aficiones:"Tortilla"}}
            ])
             */
            System.out.println("Consulta 11");
            Bson bsonFiltro = Filters.ne("aficiones", "Programación");
            Bson bsonSet = new Document("$addToSet", new Document("aficiones", "Programación"));
            UpdateResult update = coleccion2.updateMany(bsonFiltro, bsonSet);
            long modificados = update.getModifiedCount();
            System.out.println("Se han modificado: " + modificados);

            // 5. Borra las personas de "United States" que no tengan ninguna multa de 200, y muestra cuántas se han borrado.

            /*
            db.deleteMany({pais:"United States", multas:{$ne:200}})
             */
            System.out.println("Consulta 12");
            Bson bsonDelete = Filters.ne("multas", 200);
            Bson bsonPais=new Document("pais", "United States");
            Bson bsonAnd=Filters.and(bsonDelete,bsonPais);
            long borrados = coleccion2.deleteMany(bsonAnd).getDeletedCount();
            System.out.println("Se han borrado: " + borrados);
        }
    }
}