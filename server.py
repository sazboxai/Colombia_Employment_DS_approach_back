
from flask import Flask, request, jsonify
import pandas as pd
import joblib
import os
import models.filter_model as process



app = Flask(__name__)



@app.route('/municipios', methods=['GET'])
def retrieve_municipios():
    municipios = process.table_builder("municipalities")
    return {
        'status': 'OK',
        'data': municipios.to_dict()
    }

@app.route('/departamentos', methods=['GET'])
def retrieve_departamentos():
    municipios = process.table_builder("departments")
    return {
        'status': 'OK',
        'data': municipios.to_dict()
    }

@app.route('/factors/<table>', methods=['GET'])
def retrieve_factors(table):
    factors = process.table_builder(table)
    return {
        'status': 'OK',
        'data': {
            'topics': factors.to_dict()
        }
    }

@app.route('/filtered_table', methods=['POST'])
def filtered_table():
    payload = request.json
    print(payload)
    filters = process.filter_builder(payload["filters"])
    print(filters)
    data = process.table_builder(payload["table"],  filters)
    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)

@app.route('/agg_pct', methods=['POST'])
def agg_table():
    payload = request.json
    data = process.agg_builder_percent(payload["tabla"], payload["var_agg"], payload["agregador"])
    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)


@app.route('/build_count', methods=['POST'])
def builder_count():
    payload = request.json
    if "filtro" in payload:
        data = process.agg_builder_count(payload["tabla"], payload["var_agg"], payload["agregador"], payload["filtro"])
    else:
        data = process.agg_builder_count(payload["tabla"], payload["var_agg"], payload["agregador"])
    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)

@app.route('/groups_raw', methods=['POST'])
def group_r():
    payload = request.json
    if "filtro" in payload:
        data = process.group_rows(payload["tabla"], payload["var_agg"], payload["agregador"], payload["filtro"])
    else:
        data = process.group_rows(payload["tabla"], payload["var_agg"], payload["agregador"])
    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)


@app.route('/factor_x', methods=['POST'])
def fact_x():
    payload = request.json
    if "filtro" in payload:
        data = process.total_expansion(payload["tabla"], payload["var_agg"], payload["agregador"], payload["filtro"])
    else:
        data = process.total_expansion(payload["tabla"], payload["var_agg"], payload["agregador"])
    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)

@app.route('/raw_query', methods=['POST'])
def raw_query():
    payload = request.json
    data = process.table_query(payload["raw_query"])
    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)

@app.route('/employement_rate', methods=['POST'])
def employement_rate():
    payload = request.json


    if "gender" not in payload:
        genero = None
    else:
        genero =payload["gender"]

    if "month" not in payload:
        month = 12
    else:
        month =payload["month"]

    if "municipality" not in payload:
        municipality = None
    else:
        municipality =payload["municipality"]

    if "age_base" not in payload:
        age_base = None
    else:
        age_base =payload["age_base"]

    if "age_top" not in payload:
        age_top = None
    else:
        age_top =payload["age_top"]

    if "marital_status" not in payload:
        marital_status = None
    else:
        marital_status =payload["marital_status"]

    if "aggregator" not in payload:
        aggregator = None
    else:
        aggregator =payload["aggregator"]

    data = process.ocupancy_rate(genero, month, municipality, age_base, age_top,marital_status,  aggregator)

    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)

@app.route('/survival', methods=['POST'])
def survival():
    payload = request.json


    if "gender" not in payload:
        genero = None
    else:
        genero =payload["gender"]

    if "month" not in payload:
        month = 12
    else:
        month =payload["month"]

    if "municipality" not in payload:
        municipality = None
    else:
        municipality =payload["municipality"]

    if "age_base" not in payload:
        age_base = None
    else:
        age_base =payload["age_base"]

    if "age_top" not in payload:
        age_top = None
    else:
        age_top =payload["age_top"]

    if "marital_status" not in payload:
        marital_status = None
    else:
        marital_status =payload["marital_status"]

    if "aggregator" not in payload:
        aggregator = None
    else:
        aggregator =payload["aggregator"]

    if "percentil" not in payload:
        percentil = .25
    else:
        percentil =payload["percentil"]

    data = process.survival_curves(genero, month, municipality, age_base, age_top,marital_status,  aggregator, percentil)

    if data.empty==False:
        info_tabla = data.to_dict()


    else:
        info_tabla = "no info"

    obj = {
        'status': 'OK',
        'data': {
            'table':info_tabla
        }
    }
    return jsonify(obj)


@app.route('/status', methods=["GET"])
def status():
    return jsonify({"message": "ok"})


app.run(host='0.0.0.0', port=8020, debug=False )
