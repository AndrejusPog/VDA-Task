from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, substring
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import os
import shutil


def main():
    spark = SparkSession.builder.appName("VDA_Task3_Advanced").getOrCreate()

    df = spark.read.csv("combined_classified.csv", header=True)

    counts_df = (
        df.groupBy("tipas")
        .agg(count("numeris").alias("kiekis"))
        .orderBy("kiekis", ascending=False)
    )
    _save_single_csv(counts_df, "task3_result.csv", temp_dir="temp_task3")
    print("task3_result.csv created")

    invalid_df = df.filter(df["tipas"] == "nezinomas").select(
        "numeris", "failo_vardas", "tipas"
    )
    if invalid_df.count() > 0:
        _save_single_csv(invalid_df, "neteisingi_numeriai.csv", temp_dir="temp_invalid")
        print("neteisingi_numeriai.csv created")
    else:
        print("No invalid number plates found")

    df = df.withColumn("pirma_raide", substring(col("numeris"), 1, 1))

    letter_counts = df.groupBy("tipas", "pirma_raide").agg(
        count("numeris").alias("kiekis")
    )

    window_spec = Window.partitionBy("tipas").orderBy(col("kiekis").desc())

    top_letters = (
        letter_counts.withColumn("eil_nr", row_number().over(window_spec))
        .filter(col("eil_nr") <= 5)
        .orderBy("tipas", "eil_nr")
    )

    _save_single_csv(
        top_letters, "top5_pradzios_raides.csv", temp_dir="temp_topletters"
    )
    print("top5_pradzios_raides.csv created")

    spark.stop()


def _save_single_csv(df, final_filename, temp_dir):
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")

    for file in os.listdir(temp_dir):
        if file.startswith("part-") and file.endswith(".csv"):
            if os.path.exists(final_filename):
                os.remove(final_filename)
            shutil.move(os.path.join(temp_dir, file), final_filename)
            break

    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    main()
