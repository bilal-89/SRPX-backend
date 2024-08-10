from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
import json
import os
import networkx as nx


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
    degree_centrality = nx.degree_centrality(G)
    closeness_centrality = nx.closeness_centrality(G)
    betweenness_centrality = nx.betweenness_centrality(G, normalized=True, endpoints=False)
    eigenvector_centrality = nx.eigenvector_centrality(G, max_iter=1000)

    return {
        'degree': degree_centrality,
        'closeness': closeness_centrality,
        'betweenness': betweenness_centrality,
        'eigenvector': eigenvector_centrality
    }

def prepare_network_data(G):
    nodes = [{'id': node, 'label': str(node), 'value': G.degree(node)} for node in G.nodes()]
    edges = [{'from': source, 'to': target, 'value': data['weight']} for source, target, data in G.edges(data=True)]
    return {'nodes': nodes, 'edges': edges}