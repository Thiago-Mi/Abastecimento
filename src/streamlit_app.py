# streamlit_app.py
import streamlit as st
import os
import pandas as pd
from streamlit.errors import StreamlitAPIException # Import for switch_page exception

import config
from hybrid_db import HybridDBManager, Autenticador

# --- Page Configuration ---
st.set_page_config(
    page_title=config.APP_TITLE,
    page_icon=config.LOGO_PATH if config.LOGO_PATH and os.path.exists(config.LOGO_PATH) else "📊",
    layout="wide",
    initial_sidebar_state="auto" # Sidebar starts collapsed if login is shown, expands otherwise
)

# --- Initialize Session State ---
def initialize_session():
    default_states = {
        'logged_in': False,
        'username': None,
        'role': None,
        'nome_completo': None,
        'db_manager': None, # Store the manager instance here
        'data_loaded': False,
        'last_load_time': None,
        'unsaved_changes': False,
        'cliente_nome': None, # Ensure this is initialized
        # Add other state variables as needed
    }
    for key, value in default_states.items():
        if key not in st.session_state:
            st.session_state[key] = value

initialize_session()

# --- Instantiate Core Components (outside functions to persist) ---
# Create manager instance ONCE per session
if 'db_manager' not in st.session_state or st.session_state.db_manager is None:
    try:
        st.session_state.db_manager = HybridDBManager()
        # Add default admin if needed (checks Sheets directly)
        Autenticador(st.session_state.db_manager).add_default_admin_if_needed()
    except Exception as e:
        st.error("Erro crítico ao inicializar o gerenciador de banco de dados.")
        st.exception(e)
        st.stop()

if 'db_manager' in st.session_state and st.session_state.db_manager:
     authenticator = Autenticador(st.session_state.db_manager)
else:
     st.error("Erro crítico: Gerenciador de Banco de Dados não inicializado.")
     st.stop()

# --- Login Screen (Now in Main Area) ---
def show_login_screen():
    # Removed 'with st.sidebar:' - Renders in the main area now
    login_container = st.container() # Use a container for better layout control if needed
    with login_container:
        col1, col2, col3 = st.columns([1,2,1]) # Center the login form visually
        with col2:
            if config.LOGO_PATH and os.path.exists(config.LOGO_PATH):
                st.image(config.LOGO_PATH, use_container_width=True)
            else:
                st.title(config.APP_TITLE) # Show title if no logo

            st.header("Login")
            with st.form("login_form_main"):
                username = st.text_input("Usuário")
                password = st.text_input("Senha", type="password")
                submitted = st.form_submit_button("Entrar")
                if submitted:
                    with st.spinner("Verificando e carregando dados..."):
                         # Login agora também carrega os dados se for bem-sucedido
                         success, message_or_info = authenticator.login(username, password)
                    if success:
                         st.success(message_or_info) # Show success message briefly
                         st.toast("Login bem-sucedido!")
                         # Use switch_page to navigate immediately after successful login
                         try:
                             # Navigate to the main dashboard page after login
                             st.switch_page("pages/1_Visão_Geral.py")
                         except StreamlitAPIException as e:
                             st.error(f"Erro ao navegar para Visão Geral: {e}")
                             st.rerun() # Fallback to rerun if switch_page fails
                         except Exception as e: # Catch potential general errors too
                             st.error(f"Erro inesperado ao tentar navegar: {e}")
                             st.rerun()
                    else:
                         st.error(message_or_info) # Display error message


# --- Função para renderizar elementos comuns da Sidebar ---
def render_common_sidebar_elements():
    with st.sidebar:
        # Display user info
        if config.LOGO_PATH and os.path.exists(config.LOGO_PATH):
             st.image(config.LOGO_PATH, use_container_width=True)
        else:
             st.title(config.APP_TITLE)

        role = st.session_state.get('role')
        nome_completo = st.session_state.get('nome_completo')
        cliente_nome_logado = st.session_state.get('cliente_nome')

        user_display_name = cliente_nome_logado if role == 'Cliente' else nome_completo
        st.info(f"{user_display_name}")
        st.caption(f"Perfil: {role}")

        if st.session_state.get('last_load_time'):
            st.caption(f"Cache local: {st.session_state['last_load_time'].strftime('%H:%M:%S')}")
        st.divider()

        # --- Placeholder for Page-Specific Elements ---
        # Pages themselves will add their elements below this section using st.sidebar.*

        # --- Change Password Section ---
        with st.expander("Mudar Senha"):
            with st.form("change_password_form", clear_on_submit=True):
                current_password = st.text_input("Senha Atual", type="password", key="current_pw")
                new_password = st.text_input("Nova Senha", type="password", key="new_pw")
                confirm_password = st.text_input("Confirmar Nova Senha", type="password", key="confirm_pw")
                change_password_submitted = st.form_submit_button("Alterar Senha")

                if change_password_submitted:
                    if not current_password or not new_password or not confirm_password:
                        st.error("Todos os campos são obrigatórios.")
                    elif new_password != confirm_password:
                        st.error("A nova senha e a confirmação não coincidem.")
                    elif len(new_password) < config.MIN_PASSWORD_LENGTH:
                        st.error(f"A nova senha deve ter pelo menos {config.MIN_PASSWORD_LENGTH} caracteres.")
                    else:
                        username = st.session_state.get('username')
                        if username:
                            success, message = authenticator.change_password(username, current_password, new_password)
                            if success:
                                st.success(message)
                            else:
                                st.error(message)
                        else:
                            st.error("Nome de usuário não encontrado na sessão.")

        # --- Logout Button (Always at the bottom) ---
        st.divider() # Add divider before logout
        if st.button("Logout", key="logout_button_sidebar"):
             if st.session_state.get('unsaved_changes'):
                  st.warning("Você possui alterações locais não salvas!")
                  # Consider adding a confirmation step here in a real app
             else:
                  authenticator.logout()


# --- Login Screen Logic ---
def show_login_screen():
    login_container = st.container()
    with login_container:
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            if config.LOGO_PATH and os.path.exists(config.LOGO_PATH):
                st.image(config.LOGO_PATH, use_container_width=True)
            else:
                st.title(config.APP_TITLE)

            st.header("Login")
            with st.form("login_form_main"):
                username = st.text_input("Usuário")
                password = st.text_input("Senha", type="password")
                submitted = st.form_submit_button("Entrar")
                if submitted:
                    with st.spinner("Verificando e carregando dados..."):
                         success, message_or_info = authenticator.login(username, password)
                    if success:
                         st.success(message_or_info)
                         st.toast("Login bem-sucedido!")
                         try:
                             st.switch_page("pages/1_Visão_Geral.py")
                         except StreamlitAPIException as e:
                             st.error(f"Erro ao navegar para Visão Geral: {e}")
                             st.rerun()
                         except Exception as e:
                             st.error(f"Erro inesperado ao tentar navegar: {e}")
                             st.rerun()
                    else:
                         st.error(message_or_info)

# --- Main App Logic ---
if not st.session_state.get('logged_in'):
    # Hide default sidebar navigation when not logged in
    st.markdown("""
        <style>
            /* Target the sidebar section containing the page links */
            section[data-testid="stSidebar"] {
                display: none;
            }
            /* Optional: Adjust main content padding if sidebar removal causes layout shifts */
            /* .main .block-container { padding-left: 1rem; padding-right: 1rem; } */
        </style>
        """, unsafe_allow_html=True)
    # Show the login screen in the main area
    show_login_screen()
    st.stop()
else:
    # --- User is Logged In ---

    # Check data load status (important after login redirect)
    if not st.session_state.get('data_loaded'):
        st.warning("⏳ Carregando dados da sessão... Por favor, aguarde.")
        # Potentially add st.stop() if pages cannot render without data
        # st.stop()

    # Render the common sidebar elements
    render_common_sidebar_elements()

    # --- Streamlit now handles rendering the selected page ---
    # The page file itself (e.g., pages/1_Visão_Geral.py) will be executed.
    # If that page file contains `st.sidebar.*` calls, they will add elements
    # to the sidebar rendered by `render_common_sidebar_elements`.

    # Optional: Unsaved changes indicator in common sidebar?
    # Could be added inside render_common_sidebar_elements() if desired
    with st.sidebar:
        if st.session_state.get('unsaved_changes'):
            st.warning("⚠️ Alterações não salvas!")

# # --- Main App Logic ---
# if not st.session_state.get('logged_in'):
#     # If not logged in, show the login screen in the main area and stop
#     show_login_screen()
#     st.stop()
# else:
#     # --- User is Logged In ---

#     # --- Check if data finished loading (important after login redirect) ---
#     if not st.session_state.get('data_loaded'):
#         # This might briefly show if load_data_for_session is slow
#         st.warning("⏳ Carregando dados da sessão... Por favor, aguarde.")
#         # Consider adding a st.rerun() trigger if load_data_for_session uses callbacks
#         # or simply rely on Streamlit's execution flow.
#         # If data load is critical before ANY page renders, add st.stop() here.
#         # st.stop() # Uncomment if pages absolutely cannot render without data

#     # --- Sidebar for Logged-in Users (Info and Logout ONLY) ---
#     manager = st.session_state.db_manager # Get manager from session state
#     role = st.session_state.get('role')
#     username = st.session_state.get('username')
#     nome_completo = st.session_state.get('nome_completo')
#     cliente_nome_logado = st.session_state.get('cliente_nome') # Specific for Client role

#     with st.sidebar:
#         # Display user info
#         if config.LOGO_PATH and os.path.exists(config.LOGO_PATH):
#              st.image(config.LOGO_PATH, use_container_width=True)
#         else:
#              st.title(config.APP_TITLE) # Show title if no logo

#         user_display_name = cliente_nome_logado if role == 'Cliente' else nome_completo
#         st.info(f"{user_display_name}")
#         st.caption(f"Perfil: {role}")

#         if st.session_state.get('last_load_time'):
#             st.caption(f"Cache local: {st.session_state['last_load_time'].strftime('%H:%M:%S')}")
#         st.divider()

#         # --- REMOVED Filters/Actions specific to roles from here ---
#         # Filters should be placed within the sidebar context of the specific page (e.g., 1_Visão_Geral.py)
#         # st.write("Filtros específicos da página aparecerão aqui ou na página.") # Placeholder removed

#         # --- REMOVED Save Changes Button logic from here ---
#         # It was moved to 2_Abastecimento.py

#         # --- Logout Button ---
#         if st.button("Logout"):
#              # Check for unsaved changes before logging out
#              if st.session_state.get('unsaved_changes'):
#                   st.warning("Você possui alterações locais não salvas!")
#                   # Ideally, use a confirmation dialog here if available (e.g., streamlit-modal)
#                   # For now, just warn and proceed if they click again or navigate away.
#                   # Consider preventing logout completely until saved?
#                   # Let's keep the warning for now.
#              else:
#                   authenticator.logout() # Logout handles clearing session and rerun

#     # --- Main Area Content (Managed by Streamlit Pages) ---
#     # Streamlit automatically handles rendering the selected page from the `pages/` directory here.
#     # The page files themselves (1_Visão_Geral.py, 2_Abastecimento.py, etc.)
#     # are responsible for their own content and layout.

#     # Optional: Display user icon/name in top right corner (can be removed if redundant)
#     # st.markdown(f"<div style='text-align: right;'>👤 {user_display_name}</div>", unsafe_allow_html=True)
