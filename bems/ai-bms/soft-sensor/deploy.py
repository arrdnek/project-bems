from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
import mariadb
from minio import Minio
from tempfile import NamedTemporaryFile
import joblib
import numpy as np
import matplotlib.pyplot as plt
import io
from mpl_toolkits.axes_grid1 import make_axes_locatable
import time
import csv

DATABASE_PARAMETER = {
    "user": "root",
    "password": "xxx",
    "host": "xxx",
    "database": "heb_iot_alpha",
    "port": 10001
}

MINIO_PARAMETER= {
    "endpoint": "xxx:xxx",
    "access_key": "sstk",
    "secret_key": "xxx",
    "secure": False
}

ieq_params = [
    "temperature",
    "humidity",
    "wind_speed",
    "light_intensity"
]

app = FastAPI()

origins = [
    "http://xxx:10006",
    "http://xxx:10006",
    "http://xxx:10006"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"]
)

@app.get("/api/soft-sensor/{site_id}")
async def deploy(site_id: str) -> Dict[str, float | str | None]:
    overall_time = time.time()
    site_ids = []
    sites = {}
    site_alias = ""
    outdoor_id = ""
    model = {}
    metadata = {}
    input_value = {}
    model_input = {}
    result = {}

    # Initiate log
    log_filename = "./log/process_time.txt"
    log_file = open(log_filename, "a")
    log_csv = csv.writer(log_file)
    log = [time.strftime('%Y-%m-%d_%H-%M-%S')]

    # Define variables
    for param in ieq_params:
        model[param] = None
        metadata[f"{param}"] = []
        metadata[f"{param}_site"] = []

    try:
        # Establish database 
        start_time = time.time()
        connection = mariadb.connect(**DATABASE_PARAMETER)
        cursor = connection.cursor()
        log.append(time.time() - start_time)

        # Fetch site UUID
        start_time = time.time()
        query = "SELECT id, alias FROM Site WHERE id = ?"
        cursor.execute(query, (site_id,))
        try:
            site_ids = np.array(cursor.fetchall())
            for site in site_ids:
                sites[site[0]] = site[1]
            if site_id in sites.keys():
                site_alias = sites[site_id]
            elif not site_id in sites.keys():
                raise HTTPException(status_code=400, detail="The room does not exist")
            else:
                raise HTTPException(status_code=500, detail="Unknown error")
        except:
            log_file.close()
            raise HTTPException(status_code=400, detail="The room does not exist")
        query = "SELECT id FROM Site WHERE alias=? LIMIT 1"
        cursor.execute(query, ("outdoor",))
        try:
            outdoor_id = cursor.fetchall()[0][0]
        except:
            log_file.close()
            raise HTTPException(status_code=500, detail="Outdoor UUID does not exist")
        log.append(time.time() - start_time)

        # Fetch model names
        start_time = time.time()
        query = """
            WITH latest_models AS (
                SELECT model, MAX(selected_at) AS max_selected, parameter, siteId
                FROM models
                WHERE siteId = %s
                GROUP BY model
            )
            SELECT latest_models.parameter, models_metadata.model_name, models_metadata.metadata, models_metadata.meta_site
            FROM models_metadata
            JOIN latest_models ON models_metadata.model_name = latest_models.model
            WHERE latest_models.siteId = %s AND ({conditions})
        """
        conditions = " OR ".join([f"latest_models.parameter='{param}'" for param in ieq_params])
        query = query.format(conditions=conditions)
        cursor.execute(query, (site_id, site_id))
        try:
            res = cursor.fetchall()
            if len(res) > 0:
                for meta in res:
                    model[meta[0]] = meta[1]
                    metadata[meta[0]] = meta[2].split(',')
                    metadata[f"{meta[0]}_site"] = meta[3].split(',')
        except:
            log_file.close()
            raise HTTPException(status_code=500, detail="Unable to fetch models and models' metadata")
        log.append(time.time() - start_time)
        
        # Fetch input value
        start_time = time.time()
        for param in ieq_params:
            if len(metadata[param]) > 0:
                query = """
                    WITH latest_values AS (
                    SELECT v.deviceId, v.value
                    FROM Value v
                    WHERE v.created = (
                        SELECT MAX(v2.created) 
                        FROM Value v2 
                        WHERE v2.deviceId = v.deviceId)),
                    latest_stats AS (
                        SELECT s.deviceId, s.stat
                        FROM Stat s
                        WHERE s.created = (
                            SELECT MAX(s2.created) 
                            FROM Stat s2 
                            WHERE s2.deviceId = s.deviceId))
                    SELECT p.alias, lv.value
                    FROM Parameter p
                    JOIN latest_values lv ON p.id = lv.deviceId
                    WHERE p.siteId = %s AND ({conditions})
                    UNION
                    SELECT p.alias, ls.stat
                    FROM Parameter p
                    JOIN latest_stats ls ON p.id = ls.deviceId
                    WHERE p.siteId = %s AND ({conditions});
                """
                conditions = " OR ".join([f"p.alias='{meta}'" for i, meta in enumerate(metadata[param]) if metadata[f"{param}_site"][i] == "indoor"])
                query = query.format(conditions=conditions)
                cursor.execute(query, (site_id, site_id))
                try:
                    res = cursor.fetchall()
                    for meta in res:
                        input_value[meta[0]] = float(meta[1])
                except:
                    log_file.close()
                    raise HTTPException(status_code=500, detail="Error on fetching indoor parameter value")
                query = """
                    WITH latest_values AS (
                        SELECT v.deviceId, v.value
                        FROM Value v
                        WHERE v.created = (SELECT MAX(v2.created) FROM Value v2 WHERE v2.deviceId = v.deviceId)),
                    latest_stats AS (
                        SELECT s.deviceId, s.stat
                        FROM Stat s
                        WHERE s.created = (SELECT MAX(s2.created) FROM Stat s2 WHERE s2.deviceId = s.deviceId))
                    SELECT p.alias, lv.value
                    FROM Parameter p
                    JOIN latest_values lv ON p.id = lv.deviceId
                    WHERE p.siteId = %s AND ({conditions})
                    UNION
                    SELECT p.alias, ls.stat
                    FROM Parameter p
                    JOIN latest_stats ls ON p.id = ls.deviceId
                    WHERE p.siteId = %s AND ({conditions});
                """
                conditions = " OR ".join([f"p.alias='{meta}'" for i, meta in enumerate(metadata[param]) if metadata[f"{param}_site"][i] == "outdoor"])
                query = query.format(conditions=conditions)
                cursor.execute(query, (outdoor_id, outdoor_id))
                try:
                    res = cursor.fetchall()
                    for meta in res:
                        input_value[f"{meta[0]}_outdoor"] = float(meta[1])
                except:
                    log_file.close()
                    raise HTTPException(status_code=500, detail="Error on fetching outdoor parameter value")
        log.append(time.time() - start_time)

        # Load input data
        start_time = time.time()
        for param in ieq_params:
            model_input[param] = []
            if len(metadata[param]) > 0:
                for i, meta in enumerate(metadata[param]):
                    try:
                        if metadata[f"{param}_site"][i] == "outdoor":
                            model_input[param].append(input_value[f"{meta}_outdoor"])
                        elif metadata[f"{param}_site"][i] == "indoor":
                            model_input[param].append(input_value[meta])
                    except:
                        log_file.close()
                        raise HTTPException(status_code=500, detail=f"No data for {param} is unavailable")
        log.append(time.time() - start_time)

    except mariadb.Error as e:
        log_file.close()
        try:
            connection.close()
        finally:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    # Pick chosen model from MinIO
    try:
        client = Minio(**MINIO_PARAMETER)

        start_time = time.time()
        for param in ieq_params:
            if model[param] != None:
                try:
                    with NamedTemporaryFile() as tmp:
                        model[param] = client.fget_object(
                            bucket_name="heb2024",
                            object_name=f"model/{model[param]}",
                            file_path=tmp.name
                            )
                        model[param] = joblib.load(tmp.name)
                        tmp.close()
                except Exception as e:
                    log_file.close()
                    raise HTTPException(status_code=500, detail=f"Unable to load {param} model: {str(e)}")
        log.append(time.time() - start_time)
    
    except Exception as e:
        log_file.close()
        raise HTTPException(status_code=500, detail=f"Unable to fetch {param} model from MinIO: {str(e)}")
    
    # Load models and make predictions
    for param in ieq_params:
        try:
            start_time = time.time()
            if model[param] != None:
                result[param] = model[param].predict([model_input[param]])
            log.append(time.time() - start_time)
        except Exception as e:
            log_file.close()
            raise HTTPException(status_code=500, detail=f"Unable to predict {param}: {(str(e))}")

    # Create heatmap of IEQ parameters distribution
    for param in ieq_params:
        try:
            start_time = time.time()
            data = np.array(result[param]).reshape((11, 14))
            fig, ax = plt.subplots()
            im = plt.imshow(data, cmap="jet", interpolation="bicubic")
            divider = make_axes_locatable(ax)
            plt.colorbar(im, cax=divider.append_axes("right", size="3%", pad=0.1))
            ax.set_xticks([])
            ax.set_yticks([])
            plt.tight_layout()
            filename = f"{site_alias}_{param}_{time.strftime('%Y-%m-%d_%H-%M')}.png"
            buf = io.BytesIO()
            plt.savefig(buf)
            plt.close(fig)
            buf.seek(0)
            log.append(time.time() - start_time)
            start_time = time.time()
            client.put_object(
                bucket_name="heb2024",
                object_name=f"heatmap/{filename}",
                data=buf,
                length=buf.getbuffer().nbytes,
                content_type="image/png"
            )
            log.append(time.time() - start_time)
            result[f"{param}_heatmap"] = f"heatmap/{filename}"
        except:
            result[f"{param}_heatmap"] = None

    # Infer single-value of IEQ parameters
    start_time = time.time()
    for param in ieq_params:
        try:
            if param == "temperature":
                print(result[param])
                result[param] = np.round(np.mean(result[param]), 1)
            elif param == "humidity":
                result[param] = int(np.round(np.mean(result[param])))
            else:
                result[param] = np.mean(result[param])
        except:
            result[param] = None
    log.append(time.time() - start_time)
    
    # Insert result to database
    start_time = time.time()
    query = """
        INSERT INTO predict (created, deviceId, prediction)
        SELECT NOW(), Parameter.id, ?
        FROM Parameter
        WHERE Parameter.alias = ?
    """
    values = [(float(result[param]), f"ss_{param}") for param in ieq_params if result[param]]
    try:
        for val in values:
            cursor.execute(query, val)
            connection.commit()
    except mariadb.Error as e:
        log_file.close()
        try:
            connection.close()
        finally:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    log.append(time.time() - start_time)
    
    cursor.close()
    connection.close()

    log.append(time.time() - overall_time)
    log_csv.writerow(log)
    log_file.close()


    return result
