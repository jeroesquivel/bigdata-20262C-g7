"""Creación de la SparkSession con la configuración del proyecto."""
import os

from pyspark.sql import SparkSession


def get_spark(cfg, with_cassandra=False):
    """SparkSession en modo local.

    - Zona horaria UTC: todas las fechas del proyecto se interpretan en UTC.
    - shuffle.partitions = 8: con 43.200 eventos, las 200 particiones por defecto serían casi todas vacías.
    - partitionOverwriteMode = dynamic: un overwrite reemplaza solo las particiones que se escriben (D7).
    - with_cassandra=True suma spark-cassandra-connector y las credenciales de AstraDB (D9); solo lo usa
      la etapa de serving, así el resto del pipeline no necesita secretos ni descargar el conector.
    """
    sp = cfg.raw["spark"]
    builder = (SparkSession.builder
               .appName(sp["app_name"])
               .master(sp["master"])
               .config("spark.sql.session.timeZone", sp["session_timezone"])
               .config("spark.sql.shuffle.partitions", str(sp["shuffle_partitions"]))
               .config("spark.sql.sources.partitionOverwriteMode", sp["partition_overwrite_mode"])
               .config("spark.ui.showConsoleProgress", "false"))

    if sp["master"].startswith("local"):
        # La variable se lee al arrancar la JVM; las dos opciones de abajo fijan la dirección del driver.
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        # En modo local el driver y los executors son el mismo proceso. Fijar 127.0.0.1 evita que Spark
        # use la IP de la red cuando el nombre de la máquina no resuelve (pasa en algunas Mac).
        builder = (builder.config("spark.driver.host", "127.0.0.1")
                          .config("spark.driver.bindAddress", "127.0.0.1"))

    if with_cassandra:
        # Los paquetes se cargan solo al arrancar la JVM: si ya hay una sesión sin el conector,
        # getOrCreate la devolvería tal cual y la carga fallaría recién en serving.
        package = cfg.get("serving", "connector_package")
        active = SparkSession.getActiveSession()
        if active is not None and package not in active.sparkContext.getConf().get("spark.jars.packages", ""):
            raise RuntimeError(
                "Ya hay una SparkSession sin el conector de Cassandra (por ejemplo, de una ejecución anterior "
                "sin el bundle). Reiniciar la sesión de Python (en Colab: Entorno de ejecución → Reiniciar "
                "sesión) y ejecutar el notebook desde el principio.")
        from cpa.config import astra_secrets
        token, bundle = astra_secrets()
        builder = (builder
                   .config("spark.jars.packages", package)
                   # Spark copia el bundle al directorio de trabajo; el conector lo busca por nombre.
                   .config("spark.files", bundle)
                   .config("spark.cassandra.connection.config.cloud.path", os.path.basename(bundle))
                   .config("spark.cassandra.auth.username", "token")
                   .config("spark.cassandra.auth.password", token))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
