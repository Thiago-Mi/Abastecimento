# sheets_auth.py
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import config

# --- IMPORTANT: Add drive.file scope for creating new sheets ---
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file' # Ensure this is present
]

@st.cache_resource(ttl=3600) # Cache the authorized client for an hour
def get_gspread_client():
    """Authorizes gspread using Streamlit secrets and returns the client."""
    try:
        creds_dict = st.secrets["google_credentials"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)
        # Test connection by opening the main sheet
        try:
             print(f"Testando conexão com URL: {config.GOOGLE_SHEET_URL}")
             client.open_by_url(config.GOOGLE_SHEET_URL) 
             # Check if drive scope seems active (this is an indirect check)
             # A better check might be needed depending on API behavior
             print("Successfully authorized Google Sheets API.")
             # Consider adding a specific check for Drive API permissions if possible
             # For now, assume if scope is requested, it might work.
             return client
        except gspread.exceptions.SpreadsheetNotFound:
             st.error(f"Spreadsheet '{config.GOOGLE_SHEET_URL}' not found. Ensure it exists and is shared with '{creds.service_account_email}'.")
             st.stop()
        except gspread.exceptions.APIError as api_err:
            if 'drive.file' in SCOPES and 'insufficient permission' in str(api_err).lower():
                 st.warning("Possível erro de permissão para Google Drive API. A criação automática de planilhas de usuário pode falhar. Verifique as permissões da Conta de Serviço no Google Cloud.")
            st.error(f"Error opening spreadsheet '{config.GOOGLE_SHEET_URL}': {api_err}")
            st.stop()
        except Exception as e:
             st.error(f"Error opening spreadsheet '{config.GOOGLE_SHEET_URL}': {e}")
             st.stop()

    except KeyError:
        st.error("`google_credentials` not found in st.secrets. Did you create `.streamlit/secrets.toml`?")
        st.stop()
    except Exception as e:
        st.error(f"Failed to authorize Google Sheets API: {e}")
        st.stop()

# Example of how to use it:
# import sheets_auth
# gc = sheets_auth.get_gspread_client()
# spreadsheet = gc.open(config.GOOGLE_SHEET_URL)