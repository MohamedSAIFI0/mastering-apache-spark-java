package saifi.net;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        Dataset<Row> df = spark.read().option("header",true).option("inferSchema",true)
                .csv("src/main/resources/employes.csv");

        //Departement IT est salaire 14000
        df.filter(
                df.col("departement").equalTo("IT").and(df.col("salaire").geq(14000))
        ).limit(5).show();

        //les 5 employes les mieux payes, avec leur nom, prenom, departement et salaire
        df.select("nom","prenom","departement","salaire")
                .orderBy(df.col("salaire").desc())
                .show();

        //nom_complet
        df.withColumn(
                "nom_complet",
                functions.concat(df.col("nom"),lit(" "),df.col("prenom"))
        ).show();

        //categorie_salaire
        df.withColumn(
                "categorie_salaire",
                when(df.col("salaire").geq(15000),"Eleve")
                        .when(df.col("salaire").geq(10000),"Moyen")
                        .otherwise("Faible")
        ).show();


        //Valeurs manquantes
        df.filter(
                df.col("age").isNull().or(df.col("ville").isNull())
        ).show();

        df.select(
                sum(when(df.col("age").isNull(),1).otherwise(0)).alias("age_nulls"),
                sum(when(df.col("ville").isNull(),1).otherwise(0)).alias("ville_nulls")

        ).show();






        df.groupBy("departement").agg(
                count("*").alias("Le nombre d'employés"),
                avg(df.col("salaire")).alias("Le salaire moyen"),
                min(df.col("salaire")).alias("Le salaire minimum"),
                max(df.col("salaire")).alias("Le salaire maximum"),
                sum(df.col("salaire")).alias("La somme totale des salaires")
        ).show();


        df.groupBy("departement").agg(
                count("*").alias("nombre_employes")
        ).filter(
                col("nombre_employes").geq(5)
        ).show();


        df.groupBy("ville").agg(
                avg(df.col("salaire")).alias("salaire_moyen")
        ).orderBy(
                col("salaire_moyen").desc()
        ).limit(3);

        Row resultat = df.groupBy("departement").agg(
                avg(df.col("salaire")).alias("moyenne")
        ).first();
        
        double moyenne = resultat.getAs("moyenne");

        df.filter(
                col("salaire").gt(moyenne)
        ).show();


        df.groupBy("departement").agg(
                sum(when(df.col("salaire").gt(12000),1).otherwise(0)).alias("plus_12000"),
                sum(when(df.col("salaire").gt(12000),1).otherwise(0))
                        .divide(count("*")).multiply(100)
        ).show();









        WindowSpec windowSpec = Window
                .partitionBy(df.col("departement"))
                .orderBy(df.col("salaire").desc());

        Dataset<Row> employees_salary = df.withColumn(
                "rank",
                row_number().over(windowSpec)
        ).filter(
                col("rank").equalTo(1)
        );

        employees_salary.show();



        Dataset<Row> second_salary = df.withColumn(
                "rank",
                dense_rank().over(windowSpec)
        ).filter(
                col("rank").equalTo(2)
        );
        second_salary.show();

        Dataset<Row> dataset = df.withColumn(
                "rank",
                rank().over(windowSpec)
        );
        dataset.show();


        WindowSpec windowDept = Window
                .partitionBy(col("departement"));

        Dataset<Row> datasetMoyenne = df.withColumn(
                "moyenne_departement",
                round(avg(col("salaire")).over(windowDept),2)
        );

        datasetMoyenne.withColumn(
                "level",
                when(
                        col("salaire").gt(col("moyenne_departement")),
                        lit("greater")
                ).when(
                        col("salaire").lt(col("moyenne_departement")),
                        lit("lower")
                ).otherwise(lit("equal"))
        ).show();
    }
}