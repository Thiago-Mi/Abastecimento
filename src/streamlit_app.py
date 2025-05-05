# streamlit_app.py
import streamlit as st
import os
import pandas as pd

import config
from hybrid_db import HybridDBManager, Autenticador

# --- Page Configuration ---
st.set_page_config(
    page_title=config.APP_TITLE,
    page_icon=config.LOGO_PATH if config.LOGO_PATH and os.path.exists(config.LOGO_PATH) else "📊",
    layout="wide",
    initial_sidebar_state="expanded"
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
        # Add other state variables as needed (e.g., filters are now page-specific)
    }
    for key, value in default_states.items():
        if key not in st.session_state:
            st.session_state[key] = value

initialize_session()

# --- Instantiate Core Components (outside functions to persist) ---
# Create manager instance ONCE per session
# But load data only AFTER login
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

# --- Login Screen ---
def show_login_sidebar():
    with st.sidebar:
        if config.LOGO_PATH and os.path.exists(config.LOGO_PATH):
            st.image(config.LOGO_PATH, use_container_width=True)
        else:
            st.title(config.APP_TITLE) # Show title if no logo

        st.header("Login")
        with st.form("login_form_sidebar"):
            username = st.text_input("Usuário")
            password = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar")
            if submitted:
                with st.spinner("Verificando e carregando dados..."):
                     # Login agora também carrega os dados se for bem-sucedido
                     success, message_or_info = authenticator.login(username, password)
                if success:
                     st.success(message_or_info) # Show success message in sidebar briefly
                     st.toast("Login bem-sucedido!")
                     # Use switch_page to navigate immediately after successful login
                     try:
                         st.switch_page("pages/1_Visão_Geral.py")
                     except Exception as e:
                         # Handle potential error if page doesn't exist (optional but good practice)
                         st.error(f"Erro ao navegar para Visão Geral: {e}")
                         st.rerun() # Fallback to rerun if switch_page fails
                else:
                     st.error(message_or_info) # Display error message in sidebar

# --- Main App Logic ---
if not st.session_state.get('logged_in'):
    show_login_sidebar()
    st.info("Por favor, faça o login utilizando a barra lateral.")
    st.stop()
else:
    # --- Check if data finished loading (might rerun before load_data_for_session completes) ---
    if not st.session_state.get('data_loaded'):
        with st.spinner("Carregando dados da sessão..."):
            pass
         # Wait for data load completion signaled by rerun

    # --- Sidebar for Logged-in Users ---
    manager = st.session_state.db_manager # Get manager from session state
    role = st.session_state.get('role')
    username = st.session_state.get('username')
    nome_completo = st.session_state.get('nome_completo')
    cliente_nome_logado = st.session_state.get('cliente_nome') # Specific for Client role

    with st.sidebar:
        # Display user info
        if config.LOGO_PATH and os.path.exists(config.LOGO_PATH):
             st.image(config.LOGO_PATH, use_container_width=True)
        else:
             st.title(config.APP_TITLE) # Show title if no logo

        user_display_name = cliente_nome_logado if role == 'Cliente' else nome_completo
        st.info(f"{user_display_name}")
        st.caption(f"Perfil: {role}")

        if st.session_state.get('last_load_time'):
            st.caption(f"Cache local: {st.session_state['last_load_time'].strftime('%H:%M:%S')}")
        st.divider()

        # --- Sidebar Filters / Actions (Specific to Logged-in Users) ---
        if role == 'Cliente':
             period_options = ["Todos", "Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias"]
             # Ensure key is unique if used elsewhere
             st.session_state['selected_period'] = st.selectbox(
                   "Selecione Período:",
                   period_options,
                   key='client_period_select_sidebar'
             )
        elif role in ['Admin', 'Usuario']:
             # Filters specific to Admin/Usuario might be moved to 1_Visão_Geral.py's sidebar context
             st.write("Use os filtros na página 'Visão Geral'.") # Placeholder

        # --- Save Changes Button ---
        # if role == 'Usuario' and st.session_state.get('unsaved_changes'):
        #      if st.button("⚠️ Salvar Alterações na Planilha"): # Changed to st.button for sidebar context
        #           save_success = manager.save_user_data_to_sheets(st.session_state['username'])
        #           if save_success:
        #                st.success("Alterações salvas com sucesso!") # Show in sidebar
        #                st.toast("Dados enviados para a planilha!")
        #                st.session_state['unsaved_changes'] = False # Reset flag after save
        #                st.rerun() # Rerun to reflect saved state (e.g., hide button)
        #           else:
        #                st.error("Falha ao salvar alterações.") # Show in sidebar

        st.divider()

        # --- Logout Button ---
        if st.button("Logout"): # Changed to st.button for sidebar context
             if st.session_state.get('unsaved_changes'):
                  st.warning("Você possui alterações não salvas! Salve antes de sair.")
                  # TODO: Implement a modal confirmation later if possible
             else:
                  authenticator.logout() # Logout handles rerun internally

    # --- Main Area Content (Managed by Streamlit Pages) ---
    # The content of the selected page from the `pages/` directory will be displayed here.
    # You might want a default message if no page is explicitly selected or exists
    # st.info("Selecione uma opção no menu lateral.") # This might be redundant now

    # Display user icon in top right corner (Simple placeholder)
    st.markdown(f"<div style='text-align: right;'>👤 {user_display_name}</div>", unsafe_allow_html=True)
