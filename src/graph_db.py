from google.cloud import storage
from neo4j import GraphDatabase
import csv
import io
import sys

# GCP and Neo4j connection details
gcs_project_id="mapping-429718"
gcs_bucket_name = "sna-bucket-1"
gcs_file_name = "graph-db - Sheet1.csv"
neo4j_uri = "neo4j+ssc://cd191893.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "cogqlCsZR3fkdqyElSeD1rRN4IsrfJDP2P82qYgv0b8"


def test_neo4j_connection(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 AS num")
            record = result.single()
            if record and record["num"] == 1:
                print("Successfully connected to Neo4j database!")
                return True
    except Exception as e:
        print(f"Failed to connect to Neo4j: {str(e)}")
    finally:
        driver.close()
    return False


def download_csv_from_gcs(project_id, bucket_name, file_name):
    """Download CSV file from Google Cloud Storage."""
    try:
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        csv_content = blob.download_as_text()
        return csv.reader(io.StringIO(csv_content))
    except Exception as e:
        print(f"Failed to download CSV from GCS: {str(e)}")
        return None


def import_to_neo4j(uri, user, password, csv_data):
    """Import CSV data to Neo4j."""
    driver = GraphDatabase.driver(uri, auth=(user, password))

    def create_nodes_and_relationships(tx, person, program):
        tx.run("""
            MERGE (p:Person {name: $person})
            MERGE (pr:Program {name: $program})
            MERGE (p)-[:PARTICIPATES_IN]->(pr)
        """, person=person, program=program)

    try:
        with driver.session() as session:
            for row in csv_data:
                if len(row) == 2:  # Ensure the row has both person and program
                    person, program = row
                    session.execute_write(create_nodes_and_relationships, person, program)
        print("Data import completed successfully.")
    except Exception as e:
        print(f"Error during Neo4j import: {str(e)}")
    finally:
        driver.close()


def main():
    # Test Neo4j connection
    if not test_neo4j_connection(neo4j_uri, neo4j_user, neo4j_password):
        print("Exiting due to Neo4j connection failure.")
        sys.exit(1)

    # Download CSV from GCS
    csv_data = download_csv_from_gcs(gcs_project_id, gcs_bucket_name, gcs_file_name)
    if csv_data is None:
        print("Exiting due to GCS download failure.")
        sys.exit(1)

    # Skip header if present
    next(csv_data, None)

    # Import to Neo4j
    import_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, csv_data)


if __name__ == "__main__":
    main()

# from neo4j import GraphDatabase
# import sys
#
# neo4j_uri = "cd191893.databases.neo4j.io"
# neo4j_user = "neo4j"
# neo4j_password = "cogqlCsZR3fkdqyElSeD1rRN4IsrfJDP2P82qYgv0b8"
#
#
# def test_neo4j_connection(host, user, password):
#     uri_formats = [
#         f"neo4j+s://{host}",
#         f"neo4j+ssc://{host}",
#         f"bolt://{host}:7687",
#         f"bolt+s://{host}:7687",
#         f"bolt+ssc://{host}:7687"
#     ]
#
#     for uri in uri_formats:
#         print(f"Trying connection with URI: {uri}")
#         driver = GraphDatabase.driver(uri, auth=(user, password))
#         try:
#             with driver.session() as session:
#                 result = session.run("RETURN 1 AS num")
#                 record = result.single()
#                 if record and record["num"] == 1:
#                     print(f"Successfully connected to Neo4j database using {uri}!")
#                     return True
#         except Exception as e:
#             print(f"Failed to connect using {uri}: {str(e)}")
#         finally:
#             driver.close()
#     return False
#
#
# def main():
#     if not test_neo4j_connection(neo4j_uri, neo4j_user, neo4j_password):
#         print("Failed to connect to Neo4j using any of the attempted URIs.")
#         sys.exit(1)
#     print("Connection test completed.")
#
#
# if __name__ == "__main__":
#     main()