from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import GoogleAuthError

SCOPES = ['https://www.googleapis.com/auth/drive.file']

def create_drawing(service, title):
    drawing = service.files().create(body={
        'name': title,
        'mimeType': 'application/vnd.google-apps.drawing'
    }).execute()
    return drawing['id']

def add_shape(service, drawing_id, shape_type, x, y, width, height):
    requests = [{
        'createShape': {
            'objectId': f'shape_{x}_{y}',
            'shapeType': shape_type,
            'elementProperties': {
                'pageObjectId': drawing_id,
                'size': {
                    'height': {'magnitude': height, 'unit': 'PT'},
                    'width': {'magnitude': width, 'unit': 'PT'}
                },
                'transform': {
                    'translateX': x,
                    'translateY': y,
                    'unit': 'PT'
                }
            }
        }
    }]
    service.drawings().batchUpdate(drawingId=drawing_id, body={'requests': requests}).execute()

def main():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file('/Users/vincentparis/Documents/MAPPING/mapping-429718-c71cbc95e0cb.json', SCOPES)
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                print("Error: 'credentials.json' file not found.")
                print("Please ensure you've downloaded the correct OAuth 2.0 Client ID file.")
                return
            except ValueError as e:
                print(f"Error with client secrets file: {e}")
                print("Please ensure you've selected 'Desktop app' when creating your OAuth client ID.")
                return
            except GoogleAuthError as e:
                print(f"Authentication error: {e}")
                return
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    try:
        service = build('drive', 'v3', credentials=creds)

        # Create a new drawing
        drawing_id = create_drawing(service, 'Mermaid Diagram')

        # Add shapes (this is where you'd interpret the Mermaid diagram)
        add_shape(service, drawing_id, 'RECTANGLE', 100, 100, 100, 50)
        add_shape(service, drawing_id, 'RECTANGLE', 300, 100, 100, 50)

        print(f'Drawing created with ID: {drawing_id}')
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()