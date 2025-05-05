# config.py
import os
import plotly.express as px

# --- Google Sheets Configuration ---
# Replace with the actual name of your MAIN Google Sheet file
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1UmTDfLCU3FUtBnQMSR8yhmfzLBHP8uNXrd2NSjeyS9Y/"
ASSOC_COLS = ["colaborador_username", "cliente_nome"]
# Names of the CENTRAL worksheets within the main Google Sheet
SHEET_USERS = "usuarios"
SHEET_CLIENTS = "clientes"
SHEET_ASSOC = "colaborador_cliente"

# Convention for user-specific document sheets (will be prefixed)
# The user's username will be appended, e.g., "docs_diogo"
USER_DOCS_SHEET_PREFIX = "docs_"

# Expected columns for each CENTRAL sheet (ensure they match your sheet)
# Add/remove columns as needed
USERS_COLS = ["username", "hashed_password", "nome_completo", "role", "last_sync_timestamp"] # Added timestamp
CLIENTS_COLS = ["id", "nome", "tipo"] # Assuming you add a unique ID column manually to the sheet
ASSOC_COLS = ["colaborador_username", "cliente_nome"] # Example if using associations sheet

# Expected columns for the USER document sheets (adjust!)
# These MUST match the columns in your `docs_username` sheets
DOCS_COLS = [
    "id", # Recommended: Add a unique ID (UUID?) per document entry
    "colaborador_username", # For easy merging/filtering locally
    "cliente_nome",
    "data_registro", # Date of the entry itself
    "dimensao_criterio", # e.g., "Essencial", "Obrigatório", "Recomendado"
    "link_ou_documento",
    "quantidade", # Number of items this entry represents
    "status", # e.g., 'Novo', 'Enviado' (optional tracking)
    "data_envio_original", # Original upload/task date (Optional)
    # Add any other relevant columns
]


# --- Configurações da Interface ---
BASE_PATH = os.path.dirname(os.path.abspath(__file__)) # Config is at root
LOGO_PATH_RELATIVE = os.path.join("src", "images", "logo_sai.png") # Adjust path if needed
LOGO_PATH = "src/images/logo_sai.png"
if not os.path.exists(LOGO_PATH): LOGO_PATH = "logo_sai.png" # Fallback
if not os.path.exists(LOGO_PATH): LOGO_PATH = None # Or set to None if not found


# --- App Behavior ---
APP_TITLE = "SAI - Sistema Híbrido de Acesso à Informação"
DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASS = "admin" # Change in production!
DOCS_NO_DRIVE_TARGET = 54
# --- Dashboard Appearance ---
DEFAULT_BAR_COLOR = px.colors.qualitative.Plotly[0]
HIGHLIGHT_BAR_COLOR = "#636EFA"
CRITERIA_COLORS = {'Essencial': '#2ca02c', 'Obrigatório': '#ff7f0e', 'Recomendado': '#ffdd71'} # Match criteria names
DEFAULT_CRITERIA_COLOR = '#888888'

# --- Outras Configurações (Legacy/Adaptable) ---
VALID_UPLOAD_ROLES = ['Admin', 'Usuario', 'Cliente'] # Keep for potential future features
CLIENT_UPLOAD_REQUIRED_COLS = ['nome', 'tipo']
ASSOC_UPLOAD_REQUIRED_COLS = ['colaborador_username', 'cliente_nome']