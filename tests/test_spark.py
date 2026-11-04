"""Test SparkSession co Delta: ghi roi doc lai mot bang Delta tam."""

from delta.tables import DeltaTable


def test_delta_write_and_read(spark, tmp_path):
    """Ghi bang Delta, doc lai dung du lieu, co lich su phien ban."""
    path = str(tmp_path / "smoke")
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, value STRING")
    df.write.format("delta").mode("overwrite").save(path)

    back = spark.read.format("delta").load(path)
    assert sorted(r["id"] for r in back.collect()) == [1, 2]
    assert DeltaTable.isDeltaTable(spark, path)
    assert DeltaTable.forPath(spark, path).history().count() == 1


def test_spark_config(spark):
    """Cau hinh bat buoc da duoc dat."""
    conf = spark.conf
    assert conf.get("spark.sql.shuffle.partitions") == "4"
    assert "DeltaSparkSessionExtension" in conf.get("spark.sql.extensions")
    assert "DeltaCatalog" in conf.get("spark.sql.catalog.spark_catalog")
    assert "hadoop-client-api" in spark.sparkContext.getConf().get("spark.jars.excludes")
