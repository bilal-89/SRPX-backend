import ssl
from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
import json
import os
import networkx as nx
from community import community_louvain
import requests
import geopandas as gpd
from shapely.geometry import box, shape
from urllib3.exceptions import HTTPError
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Path to your CSV file
CSV_FILE_PATH = '/Users/vincentparis/Documents/MAPPING/src/mvp_data.csv'
GEOJSON_FILE_PATH = '/Users/vincentparis/Documents/MAPPING/src/data/Census_Blocks_2010.geojson'

# Load the CSV data
df = pd.read_csv(CSV_FILE_PATH, parse_dates=['Date'])

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        elif isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d')
        return super(CustomJSONEncoder, self).default(obj)

app.json_encoder = CustomJSONEncoder


def create_graph_from_df(df):
    G = nx.Graph()
    for _, row in df.iterrows():
        participant_ids = row['ParticipantIDs'].split(',')
        for i in range(len(participant_ids)):
            for j in range(i + 1, len(participant_ids)):
                if G.has_edge(participant_ids[i], participant_ids[j]):
                    G[participant_ids[i]][participant_ids[j]]['weight'] += 1
                else:
                    G.add_edge(participant_ids[i], participant_ids[j], weight=1)
    return G


def calculate_centrality_measures(G):
    if not nx.is_connected(G):
        G = G.subgraph(max(nx.connected_components(G), key=len)).copy()

    degree_centrality = nx.degree_centrality(G)
    closeness_centrality = nx.closeness_centrality(G)
    betweenness_centrality = nx.betweenness_centrality(G)
    eigenvector_centrality = nx.eigenvector_centrality(G, max_iter=1000)

    try:
        avg_shortest_path = nx.average_shortest_path_length(G)
    except nx.NetworkXError:
        avg_shortest_path = None

    communities = list(nx.community.greedy_modularity_communities(G))
    community_mapping = {}
    for i, community in enumerate(communities):
        for node in community:
            community_mapping[node] = i

    return {
        'centrality': {
            'degree': degree_centrality,
            'closeness': closeness_centrality,
            'betweenness': betweenness_centrality,
            'eigenvector': eigenvector_centrality,
            'communities': community_mapping,
            'avg_shortest_path': avg_shortest_path
        }
    }


@app.route('/api/shortest-path')
def get_shortest_path():
    start_date = request.args.get('start_date', default='2023-07-01', type=str)
    end_date = request.args.get('end_date', default='2023-07-31', type=str)
    source = request.args.get('source', type=str)
    target = request.args.get('target', type=str)

    filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
    G = create_graph_from_df(filtered_df)

    try:
        shortest_path = nx.shortest_path(G, source=source, target=target)
        path_length = len(shortest_path) - 1
        return jsonify({
            'path': shortest_path,
            'length': path_length
        })
    except nx.NetworkXNoPath:
        return jsonify({'error': 'No path exists between the selected nodes'}), 404
    except nx.NodeNotFound:
        return jsonify({'error': 'One or both of the selected nodes do not exist in the network'}), 404


def prepare_network_data(G):
    nodes = [{'id': node, 'label': str(node), 'value': G.degree(node)} for node in G.nodes()]
    edges = [{'from': source, 'to': target, 'value': data['weight']} for source, target, data in G.edges(data=True)]
    return {'nodes': nodes, 'edges': edges}


@app.route('/api/unified-data')
def get_unified_data():
    start_date = request.args.get('start_date', default='2023-07-01', type=str)
    end_date = request.args.get('end_date', default='2023-07-31', type=str)
    activity_type = request.args.get('activity_type', default=None, type=str)
    cluster = request.args.get('cluster', default=None, type=str)

    filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

    if activity_type:
        filtered_df = filtered_df[filtered_df['ActivityType'] == activity_type]

    if cluster:
        filtered_df = filtered_df[filtered_df['ClusterName'] == cluster]

    result = filtered_df.replace({np.nan: None}).to_dict('records')

    enhanced_result = []
    for activity in result:
        enhanced_activity = {
            **activity,
            'Sequence': [
                {
                    'Action': 'Study',
                    'Material': activity['MaterialUsed']
                },
                {
                    'Action': 'Consult',
                    'Topic': activity['ServiceProjectType'] if activity['ActivityType'] in ['JYG', 'Nucleus'] else 'General'
                },
                {
                    'Action': 'Reflect',
                    'Topic': 'Activity Impact'
                },
                {
                    'Action': 'Act',
                    'Topic': activity['ServiceProjectType'] if activity['ActivityType'] in ['JYG', 'Nucleus'] else 'Apply Learnings'
                }
            ]
        }
        enhanced_result.append(enhanced_activity)

    return jsonify(enhanced_result)


@app.route('/api/network-data')
def get_network_data():
    start_date = request.args.get('start_date', default='2023-07-01', type=str)
    end_date = request.args.get('end_date', default='2023-07-31', type=str)

    filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

    # Create nodes (unique participants)
    nodes = filtered_df[['ParticipantID', 'ParticipantName', 'ParticipantRole']].drop_duplicates().replace(
        {np.nan: None}).to_dict('records')

    # Create edges (connections through activities)
    edges = []
    for _, group in filtered_df.groupby(['Date', 'ActivityType', 'Latitude', 'Longitude']):
        participants = group['ParticipantID'].tolist()
        for i in range(len(participants)):
            for j in range(i + 1, len(participants)):
                edges.append({'source': participants[i], 'target': participants[j]})

    return jsonify({'nodes': nodes, 'edges': edges})


@app.route('/api/geo-data')
def get_geo_data():
    start_date = request.args.get('start_date', default='2023-07-01', type=str)
    end_date = request.args.get('end_date', default='2023-07-31', type=str)

    filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

    # Group by unique locations and activities
    geo_data = filtered_df.groupby(['Latitude', 'Longitude', 'ActivityType']).agg({
        'ActivitySize': 'mean',
        'ParticipantID': 'count'
    }).reset_index()

    geo_data = geo_data.rename(columns={'ParticipantID': 'ParticipantCount'})
    geo_data = geo_data.replace({np.nan: None})

    return jsonify(geo_data.to_dict('records'))


@app.route('/api/centrality-measures')
def get_centrality_measures():
    start_date = request.args.get('start_date', default='2023-07-01', type=str)
    end_date = request.args.get('end_date', default='2023-07-31', type=str)

    filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

    G = create_graph_from_df(filtered_df)
    centrality_measures = calculate_centrality_measures(G)
    network_data = prepare_network_data(G)

    result = {
        'centrality': centrality_measures,
        'network': network_data
    }

    return jsonify(result)


def get_census_data_logic(min_lat, max_lat, min_lon, max_lon):
    try:
        app.logger.info(f"Attempting to read file: {GEOJSON_FILE_PATH}")
        if not os.path.exists(GEOJSON_FILE_PATH):
            app.logger.error(f"File not found: {GEOJSON_FILE_PATH}")
            return {"error": "Census data file not found"}, 404

        bbox = box(min_lon, min_lat, max_lon, max_lat)
        gdf = gpd.read_file(GEOJSON_FILE_PATH, mask=bbox)

        app.logger.info(f"Successfully read data. Number of features: {len(gdf)}")

        # Ensure required columns exist and calculate if necessary
        if 'population' not in gdf.columns or 'ALAND10' not in gdf.columns:
            app.logger.warning("Required columns missing. Unable to calculate population density.")
            gdf['population_density'] = 0
        else:
            gdf['population_density'] = gdf['population'] / (gdf['ALAND10'] / 1000000)  # per square km

        # Ensure median age and income are present (adjust field names as necessary)
        gdf['medianAge'] = gdf['medianAge'] if 'medianAge' in gdf.columns else 0
        gdf['median_income'] = gdf['median_income'] if 'median_income' in gdf.columns else 0

        # Convert to GeoJSON
        geojson_data = gdf.to_crs(epsg=4326).__geo_interface__

        app.logger.info("Successfully processed GeoJSON data")
        return geojson_data, 200

    except Exception as e:
        app.logger.error(f"Error processing census data: {str(e)}")
        return {"error": f"Failed to process census data: {str(e)}"}, 500


@app.route('/api/census-data')
def get_census_data():
    app.logger.info("Received request for census data")

    try:
        # Get bounding box from query parameters
        min_lat = float(request.args.get('min_lat', 39.9))
        max_lat = float(request.args.get('max_lat', 40.1))
        min_lon = float(request.args.get('min_lon', -75.2))
        max_lon = float(request.args.get('max_lon', -75.0))

        app.logger.info(f"Bounding box: {min_lat}, {max_lat}, {min_lon}, {max_lon}")

        data, status_code = get_census_data_logic(min_lat, max_lat, min_lon, max_lon)
        return jsonify(data), status_code

    except ValueError as e:
        app.logger.error(f"Invalid parameters: {str(e)}")
        return jsonify({"error": "Invalid parameters. Please provide valid numeric values for the bounding box."}), 400


@app.route('/api/growth-rates', methods=['GET'])
def get_growth_rates():
    try:
        start_date = request.args.get('start_date', default='2023-07-01', type=str)
        end_date = request.args.get('end_date', default='2023-07-31', type=str)

        filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

        if filtered_df.empty:
            return jsonify({"error": "No data available for the specified date range"}), 404

        growth_rates = {
            'overall': calculate_growth_rate(filtered_df),
            'by_week': calculate_growth_rate_by_period(filtered_df, 'W'),
            'by_activity_type': calculate_growth_rate_by_group(filtered_df, 'ActivityType')
        }

        return jsonify(growth_rates)
    except Exception as e:
        app.logger.error(f"Error calculating growth rates: {str(e)}")
        return jsonify({"error": f"Failed to calculate growth rates: {str(e)}"}), 500


def calculate_growth_rate(df):
    start_value = df.groupby('Date')['ActivitySize'].sum().iloc[0]
    end_value = df.groupby('Date')['ActivitySize'].sum().iloc[-1]
    num_periods = (df['Date'].max() - df['Date'].min()).days / 7  # Assuming weekly rate

    if start_value == 0:
        return 0

    growth_rate = (end_value / start_value) ** (1 / num_periods) - 1
    return float(growth_rate)  # Convert to float to ensure JSON serialization


def calculate_growth_rate_by_period(df, period):
    df_grouped = df.groupby(pd.Grouper(key='Date', freq=period))['ActivitySize'].sum()
    growth_rates = df_grouped.pct_change()
    return float(growth_rates.mean())  # Convert to float to ensure JSON serialization


def calculate_growth_rate_by_group(df, group_column):
    df_grouped = df.groupby([group_column, pd.Grouper(key='Date', freq='W')])['ActivitySize'].sum().unstack()
    growth_rates = (df_grouped.iloc[-1] / df_grouped.iloc[0]) ** (1 / df_grouped.shape[1]) - 1
    return {str(k): float(v) for k, v in growth_rates.items()}  # Convert keys to strings and values to floats


if __name__ == '__main__':
    app.run(debug=True, port=5001)