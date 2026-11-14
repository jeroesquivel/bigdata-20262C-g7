"""Rutas del lago, escritura en Parquet y estadísticas de archivos."""
import os
from pathlib import Path


def zone_path(cfg, zone, *parts):
    """Ruta dentro de una zona, con el patrón <zona>/<entidad>/<particion>=<valor>/."""
    return str(Path(cfg.paths[zone], *parts))


def _check_not_landing(cfg, path):
    """Landing es de solo lectura: cualquier escritura que apunte ahí es un error de programación."""
    landing = Path(cfg.paths["landing"]).resolve()
    target = Path(path).resolve()
    if target == landing or landing in target.parents:
        raise PermissionError(f"Escritura bloqueada: {path} está dentro de Landing")


def write_parquet(cfg, df, path, mode="overwrite", partition_by=None, num_files=None, replace_all=False):
    """Escribe Parquet (snappy, el formato por defecto de Spark, D3).

    num_files fija la cantidad de archivos por partición con coalesce: con la muestra cada salida pesa
    pocos KB y sin esto Spark escribiría un archivo por tarea (problema de archivos chicos).
    Con partitionOverwriteMode=dynamic, mode="overwrite" reemplaza solo las particiones presentes en `df`.
    replace_all=True reemplaza la tabla completa (Silver y Gold se recalculan enteros en el MVP, D7): así una
    partición que ya no corresponde no queda vieja en disco.
    """
    _check_not_landing(cfg, path)
    if num_files is not None:
        df = df.coalesce(num_files)
    writer = df.write.mode(mode)
    if replace_all:
        writer = writer.option("partitionOverwriteMode", "static")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def path_stats(path):
    """Cantidad de archivos de datos y bytes bajo una ruta (ignora _SUCCESS y los .crc de Spark)."""
    files, size = 0, 0
    for root, _, names in os.walk(path):
        for name in names:
            if name.startswith(("_", ".")):
                continue
            files += 1
            size += os.path.getsize(os.path.join(root, name))
    return {"path": str(path), "files": files, "bytes": size}


def source_file_col(cfg):
    """Columna `source_file`: ruta del archivo de origen relativa a Landing (linaje, sección 6.4).

    `_metadata.file_path` trae una URI completa y codificada (file:/.../Big%20data/...). Se decodifica y se
    le quita el prefijo de Landing, así queda "customers_orgs.csv" o "usage_events_stream/events_part_0000.jsonl"
    y no depende de la máquina donde se corrió.
    """
    import re
    from pyspark.sql import functions as F
    prefix = "^file:" + re.escape(str(Path(cfg.paths["landing"]).resolve())) + "/"
    return F.regexp_replace(F.url_decode(F.col("_metadata.file_path")), prefix, "")


GENERATED_ZONES = ("bronze", "silver", "gold", "quarantine", "checkpoints")


def reset_lake(cfg, include_meta=False):
    """Borra las zonas generadas. Bronze de eventos y el checkpoint del stream se borran siempre juntos:
    con solo el checkpoint borrado el stream volvería a leer todo y duplicaría Bronze; con solo Bronze borrado
    el checkpoint diría que no hay nada pendiente y Bronze quedaría vacío. Landing no se toca nunca."""
    import shutil
    zones = GENERATED_ZONES + (("meta",) if include_meta else ())
    landing = Path(cfg.paths["landing"]).resolve()
    removed = []
    for zone in zones:
        path = Path(cfg.paths[zone]).resolve()
        if path == landing or path in landing.parents:   # nunca borrar Landing ni una carpeta que la contenga
            raise PermissionError(f"{zone} apunta a Landing o la contiene: {path}")
        if path.exists():
            shutil.rmtree(path)
            removed.append(str(path))
    return removed
