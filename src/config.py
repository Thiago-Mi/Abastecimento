# --- START OF FILE config.py ---

import os
import plotly.express as px

# --- Configurações do Banco de Dados ---
DB_FILE = "app_database.db"
DEFAULT_BAR_COLOR = px.colors.qualitative.Plotly[0]
HIGHLIGHT_BAR_COLOR = "#636EFA"
CRITERIA_COLORS = {'Critérios Essenciais': '#2ca02c', 'Obrigatórios': '#ff7f0e', 'Recomendados': '#ffdd71'}
DEFAULT_CRITERIA_COLOR = '#888888'

# --- Configurações da Interface ---
# Tenta construir um caminho relativo mais robusto assumindo que 'config.py'
# está na raiz ou em um local conhecido em relação a 'src'
# Ajuste este base_path conforme a estrutura real do seu projeto.
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Vai dois níveis acima de config.py
# Ou, se config.py estiver na raiz: BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Use os.path.join para criar caminhos de forma segura
LOGO_PATH_RELATIVE = os.path.join("src", "images", "logo_sai.png")
LOGO_PATH = os.path.join(BASE_PATH, LOGO_PATH_RELATIVE) # Caminho completo

# Verifica se o logo existe no caminho construído
if not os.path.exists(LOGO_PATH):
    # Tenta um caminho relativo simples caso a estrutura seja diferente
    simple_relative_path = LOGO_PATH_RELATIVE
    if os.path.exists(simple_relative_path):
        LOGO_PATH = simple_relative_path
    else:
        # Como último recurso, usa apenas o nome do arquivo (assume que está no mesmo dir)
        # ou deixa como está para mostrar o warning no Streamlit
        potential_alt_path = "logo_sai.png"
        if os.path.exists(potential_alt_path):
             LOGO_PATH = potential_alt_path
        # else: Mantenha o LOGO_PATH calculado inicialmente para que o warning no streamlit.py mostre onde procurou

# --- Outras Configurações ---
APP_TITLE = "SAI - Sistema de Acesso à Informação"
VALID_UPLOAD_ROLES = ['Admin', 'Usuario', 'Cliente']
CLIENT_UPLOAD_REQUIRED_COLS = ['nome', 'tipo']
ASSOC_UPLOAD_REQUIRED_COLS = ['colaborador_username', 'cliente_nome']

# --- END OF FILE config.py ---