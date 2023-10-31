from flask import Flask, request, jsonify, make_response
import mysql.connector
import os
import logging
from copy import deepcopy

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

VERSION="1.0.0-b1"

MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE')
MYSQL_PORT = os.environ.get('MYSQL_PORT') or 3306

#ALLOWED_ORIGIN = ['http://127.0.0.1:5173', 'https://archivista-ng.provincia.lucca.it']
ALLOWED_ORIGIN = ['https://archivista-ng.provincia.lucca.it']

def mysql_search(words, logic):
    logger = logging.getLogger()
    query_results = []
    results = {}
    
    logger.debug(f"MySQL User: {MYSQL_USER}")

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT
    )

    cursor = conn.cursor(dictionary=True)

    only_full_group_by = "SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
    cursor.execute(only_full_group_by)   

    query = """ 
            SELECT u.id as unit_id, f.name as fond_name, u.fond_id as fond_id, u.title as title, u.content as content, u.arrangement_note as note, t.isri as textual, v.stmd as visual, s.sgti as extended 
            FROM 
            `units` u 
            join fonds f on u.root_fond_id = f.id
            left join sc2_textual_elements t on u.id = t.unit_id 
            left join sc2_visual_elements v on u.id = v.unit_id
            left join sc2s s on u.id = s.unit_id
            where 
            u.published = 1 
            AND 
            (u.title like %s
            OR
            u.content like %s
            OR
            u.arrangement_note like %s
            OR
            t.isri like %s
            OR 
            v.stmd like %s
            OR 
            s.sgti like %s);"""

    for word in words:
        logger.debug(f"Searching for {word}")
        valori = ("%"+word+"%", "%"+word+"%", "%"+word+"%", "%"+word+"%", "%"+word+"%", "%"+word+"%")
        cursor.execute(query, valori)
        query_result = cursor.fetchall()

        logger.debug(f"Found {len(query_result)} items")
        for item in query_result:
            query_results.append(item)
        

    logger.debug(f"All results : {query_results}")

    temp_results = {}
    for item in query_results:
        logger.debug(f"Working on : {item} ")
        logger.debug(f"item unit_id: {item['unit_id']}")
        if (item["unit_id"] not in temp_results):
            logger.debug(f"Item not present")
            unit = {
                "unit_id" : item["unit_id"],
                "fond_name" : item["fond_name"],
                "fond_id" : item["fond_id"],
                "title": item["title"],
                "content" : item["content"],
                "extra" : "",
                "count" : 0
            }

            temp_results[item["unit_id"]] = unit            
           
        unit = temp_results[item["unit_id"]] 
        unit["count"] += 1

        if (unit["count"] == 1):
            if (item["note"] is not None):
                if (len(item["note"]) != 0):
                    logger.debug(f"item[note] : {item['note']}")
                    if (len(unit["extra"]) == 0):
                        unit["extra"] += item["note"]
                    else:
                        unit["extra"] += "<br />" + item["note"]

        if (item["textual"] is not None):
            if (len(item["textual"]) != 0):
                logger.debug(f"item[textual] : {item['textual']}")
                if (len(unit["extra"]) == 0):
                    unit["extra"] += item["textual"]
                else:
                    unit["extra"] += "<br />" + item["textual"]

        if (item["visual"] is not None):
            if (len(item["visual"]) != 0):
                logger.debug(f"item[visual] : {item['visual']}")
                if (len(unit["extra"]) == 0):
                    unit["extra"] += item["visual"]
                else:
                    unit["extra"] += "<br />" + item["visual"]

        if (item["extended"] is not None):
            if (len(item["extended"]) != 0):
                logger.debug(f"item[extended] : {item['extended']}")
                if (len(unit["extra"]) == 0):
                    unit["extra"] += item["extended"]
                else:
                    unit["extra"] += "<br />" + item["extended"]

    results = {}
    if (logic == 'and'):
        logger.debug(f"{temp_results}")
        for item in temp_results:
            unit = temp_results[item]
            logger.debug(f"hhh {len(words)}")
            if int(unit["count"]) >= len(words):
                results[item] = unit
    else:
        results = temp_results

    cursor.close()
    conn.close()

    return results

class MissingElement(Exception):
    pass

def _build_cors_preflight_response(origin):
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", origin)
    response.headers.add('Access-Control-Allow-Headers', "*")
    response.headers.add('Access-Control-Allow-Methods', "*")
    return response

def _corsify_actual_response(response, origin):
    response.headers.add("Access-Control-Allow-Origin", origin)
    return response

def origin_allowed(origin):
    allowed = False

    if (origin is not None):
        if origin in ALLOWED_ORIGIN:
            allowed = True

    return allowed


@app.route('/archi-search', methods=['POST', 'OPTIONS'])
def esegui_ricerca():
    result = {}
    logger = logging.getLogger()
    origin = None

    try:
        origin = request.headers.get('Origin')
        logger.info(f"Origin : {origin}")

        if origin_allowed(origin):
            if request.method == "OPTIONS": # CORS preflight
                return _build_cors_preflight_response(origin)
            elif request.method == "POST": 
        
                request_data = request.get_json()

                if "words" not in request_data:
                    raise MissingElement("Lista parole mancante")
                
                if "logic" not in request_data:
                    raise MissingElement("Logica di ricerca mancante")
                
                words = request_data['words']
                logic = request_data['logic'].lower()
                
                data = mysql_search(words, logic)

                result['status'] = 'ok'
                result['data'] = data
            else:
                raise RuntimeError("Weird - don't know how to handle method {}".format(request.method))
        
    
    except Exception as error:
        logger.error(f"Error : {repr(error)}")
        result['status'] = "error"
        result['message'] = repr(error)
     
    logger.debug(f"Result : {result}")
    return _corsify_actual_response(jsonify(result), origin)
    #return jsonify(risultati)

if __name__ == "__main__":
    #app.run(ssl_context=('tls.crt','tls.key'), host='0.0.0.0')
    app.run(host='0.0.0.0')