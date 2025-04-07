from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import os
import shutil


def classify_vehicle_type(plate):
    if not plate:
        return "nezinomas"
    plate = plate.strip().upper()

    if plate.startswith("T") and plate[1:].isdigit() and len(plate) == 6:
        return "taksi"
    elif (
        plate.startswith("E")
        and len(plate) == 6
        and plate[1].isalpha()
        and plate[2:].isdigit()
    ):
        return "elektromobilis"
    elif plate.startswith("H") and plate[1:].isdigit() and len(plate) in [5, 6]:
        return "istorinis"
    elif plate.startswith("P") and plate[1:].isdigit() and len(plate) == 6:
        return "komercinis"
    elif plate.isdigit() and len(plate) in [5, 6]:
        return "diplomatinis"
    elif any(char.isdigit() for char in plate):
        if len(plate) <= 4:
            return "mopedas"
        elif len(plate) <= 5:
            return "motociklas"
        elif len(plate) == 6:
            return "lengvasis"
        elif len(plate) > 6:
            return "priekaba"
    return "nezinomas"


vehicle_type_udf = udf(classify_vehicle_type, StringType())


def main():
    spark = SparkSession.builder.appName("VDA_NumberPlatePipeline").getOrCreate()

    df1 = spark.read.csv("df1.csv", header=True).withColumn("failo_vardas", lit("df1"))
    df2 = spark.read.csv("df2.csv", header=True).withColumn("failo_vardas", lit("df2"))
    df3 = spark.read.csv("df3.csv", header=True).withColumn("failo_vardas", lit("df3"))

    combined_df = df1.union(df2).union(df3)

    _save_single_csv(combined_df, "combined.csv", temp_dir="temp_combined")
    print("combined.csv created")

    combined_df_unique = combined_df.dropDuplicates(["numeris"])
    _save_single_csv(combined_df_unique, "combined_unique.csv", temp_dir="temp_unique")
    print("combined_unique.csv created (duplicates removed)")

    df_classified = combined_df_unique.withColumn(
        "tipas", vehicle_type_udf(combined_df_unique["numeris"])
    )

    window_spec = Window.orderBy("numeris")
    df_classified = df_classified.withColumn("eil_nr", row_number().over(window_spec))

    _save_single_csv(
        df_classified, "combined_classified.csv", temp_dir="temp_classified"
    )
    print("combined_classified.csv created with transport type and row numbers")

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
