# pages/3_Administração.py
import streamlit as st
import pandas as pd
from datetime import datetime
import config
import gspread # Need gspread exceptions

st.set_page_config(layout="wide")

# --- Check Login and Role ---
if not st.session_state.get('logged_in'):
    st.error("Por favor, faça o login para acessar esta página.")
    st.stop()
if st.session_state.get('role') != 'Admin':
    st.error("Apenas Administradores podem acessar esta página.")
    st.stop()
if not st.session_state.get('data_loaded') or not st.session_state.get('db_manager'):
    st.warning("Os dados ainda estão sendo carregados ou o gerenciador não foi inicializado.")
    st.stop()

manager = st.session_state.db_manager
admin_username = st.session_state.get('username')
admin_role = st.session_state.get('role')


# --- Page Title ---
st.markdown("#### 👑 Painel de Administração")
st.divider()

# --- Tabs for Navigation ---
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Visão Geral",
    "👤 Cadastrar Usuário",
    "🏢 Cadastrar Cliente",
    "🔗 Atribuir Cliente-Colaborador"
])

# ==========================
# Tab 1: Visão Geral
# ==========================
with tab1:
    st.subheader("Visão Geral dos Usuários e Sincronização")
    users_data = manager.get_all_users_local_with_sync()

    if users_data:
        df_users = pd.DataFrame([dict(row) for row in users_data])

        def format_sync_time(ts_str):
            if not ts_str or ts_str == 'None': return "Nunca"
            try:
                dt_obj = datetime.fromisoformat(ts_str.replace(' ', 'T'))
                return dt_obj.strftime("%d/%m/%Y %H:%M:%S")
            except (ValueError, TypeError):
                 return str(ts_str) # Show raw value if parsing fails

        df_users['Última Sincronização'] = df_users['last_sync_timestamp'].apply(format_sync_time)
        cols_display = ['nome_completo', 'username', 'role', 'Última Sincronização']
        st.dataframe(df_users[[col for col in cols_display if col in df_users.columns]], use_container_width=True)
        st.info(f"Total de usuários no cache local: {len(df_users)}")
    else:
        st.warning("Nenhum dado de usuário encontrado no cache local.")

    st.divider()
    st.subheader("Ações Gerais")

    if st.button("🔄 Recarregar Todos os Dados das Planilhas", key="reload_all_data"):
         with st.spinner("Recarregando todos os dados..."):
            try:
                manager.load_data_for_session(admin_username, admin_role)
                st.success("Dados recarregados com sucesso!")
                st.rerun()
            except Exception as e:
                st.error("Falha ao recarregar os dados.")
                st.exception(e)

    st.divider()
    st.subheader("KPIs Gerais (Todos Usuários - Cache Local)")
    kpi_geral = manager.get_kpi_data_local()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Registrado", f"{kpi_geral.get('enviados', 0):02d}")
    col2.metric("Total Validado", f"{kpi_geral.get('validados', 0):02d}")
    col3.metric("Total Pendente", f"{kpi_geral.get('pendentes', 0):02d}")
    col4.metric("Total Inválido", f"{kpi_geral.get('invalidos', 0):02d}")
    

    st.divider()
    st.subheader("Pontuação Atualizada (Direto das Planilhas)")
    if st.button("📊 Calcular Pontuação Atualizada (Lento)"):
        with st.spinner("Buscando dados e calculando pontuação das planilhas..."):
            # Limpar cache específico desta função antes de chamar? Opcional.
            # hybrid_db.HybridDBManager.calcular_pontuacao_colaboradores_gsheet.clear()
            df_pontuacao_gsheet = manager.calcular_pontuacao_colaboradores_gsheet()
        if not df_pontuacao_gsheet.empty:
            st.dataframe(df_pontuacao_gsheet)
        else:
            st.warning("Não foi possível calcular a pontuação diretamente das planilhas ou não há dados.")
     
# ==========================
# Tab 2: Cadastrar Usuário
# ==========================
with tab2:
    st.subheader("Cadastrar Novo Usuário no Sistema")

    with st.form("new_user_form", clear_on_submit=True):
        st.markdown("**Informações do Usuário**")
        new_username = st.text_input("Nome de Usuário (Login)", key="nu_uname").strip()
        new_fullname = st.text_input("Nome Completo", key="nu_fname").strip()
        new_password = st.text_input("Senha Temporária", type="password", key="nu_pass")
        new_role = st.selectbox("Perfil (Role)", ["Usuario", "Admin", "Cliente"], key="nu_role") # List options

        submitted = st.form_submit_button("✨ Cadastrar Usuário")

        if submitted:
            # --- Basic Validations ---
            if not all([new_username, new_fullname, new_password, new_role]):
                st.error("❌ Por favor, preencha todos os campos.")
            else:
                 with st.spinner(f"Verificando e cadastrando '{new_username}'..."):
                    # --- Check for Duplicate Username (Directly in Google Sheets) ---
                    is_duplicate = False
                    try:
                        users_ws = manager._get_worksheet(config.SHEET_USERS)
                        if users_ws:
                             # Using find is simple but might be slow on huge sheets
                             cell = users_ws.find(new_username, in_column=1) # Assumes username is col 1
                             if cell:
                                  is_duplicate = True
                                  st.error(f"❌ Erro: Nome de usuário '{new_username}' já existe!")
                        else:
                             st.error("❌ Erro: Não foi possível acessar a planilha de usuários para verificação.")
                             # Stop execution if worksheet is inaccessible
                             st.stop()
                    except gspread.exceptions.APIError as api_err:
                         st.error(f"❌ Erro de API ao verificar usuário: {api_err}")
                         # Stop execution on API error
                         st.stop()
                    except Exception as find_err:
                         st.error(f"❌ Erro inesperado ao verificar duplicidade: {find_err}")
                         # Stop execution on unexpected error
                         st.stop()


                    # --- Proceed if not duplicate ---
                    if not is_duplicate:
                         hashed_pw = manager._hash_password(new_password)
                         # Match order of columns in config.USERS_COLS
                         user_data_list = [
                              new_username,
                              hashed_pw,
                              new_fullname,
                              new_role,
                              None # last_sync_timestamp starts as None
                         ]
                         # Slice to ensure correct number of columns just in case
                         user_data_to_append = user_data_list[:len(config.USERS_COLS)]

                         # --- Append User to Google Sheet ---
                         user_added_success = False
                         try:
                              users_ws.append_row(user_data_to_append, value_input_option='USER_ENTERED')
                              st.success(f"✅ Usuário '{new_username}' ({new_role}) adicionado à planilha principal.")
                              user_added_success = True
                         except Exception as append_err:
                              st.error(f"❌ Falha ao adicionar usuário '{new_username}' na planilha: {append_err}")

                         # --- Create User Document Sheet (if role is 'Usuario') ---
                         sheet_created_or_not_needed = False
                         if user_added_success and new_role == 'Usuario':
                              docs_sheet_name = manager._get_user_sheet_name(new_username)
                              st.write(f"Tentando criar planilha de documentos '{docs_sheet_name}'...")
                              try:
                                   # Check if it exists first (optional, but good practice)
                                   try:
                                        existing_ws = manager.spreadsheet.worksheet(docs_sheet_name)
                                        st.warning(f"⚠️ Planilha '{docs_sheet_name}' já existe. Não será recriada.")
                                        sheet_created_or_not_needed = True
                                   except gspread.exceptions.WorksheetNotFound:
                                        # Expected case: Sheet doesn't exist, try to create
                                        new_ws = manager.spreadsheet.add_worksheet(
                                             title=docs_sheet_name,
                                             rows=10, # Start small
                                             cols=len(config.DOCS_COLS)
                                        )
                                        # Add header row immediately
                                        new_ws.update([config.DOCS_COLS], value_input_option='USER_ENTERED')
                                        st.success(f"✅ Planilha '{docs_sheet_name}' criada com sucesso.")
                                        sheet_created_or_not_needed = True

                              except gspread.exceptions.APIError as drive_api_err:
                                   error_message = f"❌ Falha ao criar planilha '{docs_sheet_name}'. Erro de API: {drive_api_err}"
                                   if 'drive.file' not in config.SCOPES: # Checking our config, not actual permissions
                                        error_message += "\n\nA scope 'drive.file' pode estar faltando em `sheets_auth.py`."
                                   elif 'insufficient permission' in str(drive_api_err).lower():
                                       error_message += "\n\nVerifique as permissões da Conta de Serviço no Google Cloud. Precisa de permissão para editar arquivos no Google Drive."
                                   st.error(error_message)
                                   st.warning("O usuário foi criado, mas você precisará criar e configurar a planilha de documentos manualmente.")
                              except Exception as create_sheet_err:
                                   st.error(f"❌ Erro inesperado ao criar planilha '{docs_sheet_name}': {create_sheet_err}")
                                   st.warning("O usuário foi criado, mas você precisará criar e configurar a planilha de documentos manualmente.")
                         elif new_role != 'Usuario':
                                sheet_created_or_not_needed = True # No sheet needed for Admin/Cliente

                         # --- Reload Local Data after Successful Changes ---
                         if user_added_success: # Reload only if the primary user addition was successful
                              st.info("Atualizando cache de dados local...")
                              try:
                                   # Use the existing load function to refresh local state
                                   manager.load_data_for_session(admin_username, admin_role)
                                   st.success("Cache local atualizado.")
                                   st.info("O novo usuário agora está visível na 'Visão Geral'.")
                                   # No st.rerun() needed here as form clears and feedback is shown.
                                   # User can switch tabs to see the update.
                              except Exception as reload_err:
                                   st.error("Usuário adicionado às planilhas, mas falha ao recarregar o cache local. Recarregue a página ou use o botão 'Recarregar Todos os Dados'.")
                                   st.exception(reload_err)


# ==========================
# Tab 3: Cadastrar Cliente
# ==========================
with tab3:
    st.subheader("Cadastrar Novo Cliente no Sistema")

    with st.form("new_client_form", clear_on_submit=True):
        st.markdown("**Informações do Cliente**")
        new_client_name = st.text_input("Nome do Cliente", key="nc_name").strip()
        # Pode adicionar tipos padrão ou buscar tipos existentes
        tipos_existentes = list(set([c['tipo'] for c in manager.listar_clientes_local() if c['tipo']]))
        tipos_opcao = sorted(list(set(["Prefeitura", "Câmara", "Autarquia", "Outro"] + tipos_existentes)))
        new_client_type = st.selectbox("Tipo de Cliente", tipos_opcao, key="nc_type")

        submit_new_client = st.form_submit_button("🏢 Cadastrar Cliente")

        if submit_new_client:
            if not new_client_name or not new_client_type:
                 st.error("❌ Por favor, preencha todos os campos.")
            else:
                 with st.spinner(f"Cadastrando cliente '{new_client_name}'..."):
                    # Use the new method in manager to add to both local and gsheet
                    success = manager.add_cliente_local_and_gsheet(new_client_name, new_client_type)
                    if success:
                        # Reload local data to reflect the new client
                        st.info("Atualizando cache local...")
                        try:
                             manager.load_data_for_session(admin_username, admin_role)
                             st.success("Cache local atualizado.")
                             st.info("O novo cliente agora está disponível.")
                        except Exception as reload_err:
                             st.error("Cliente adicionado, mas falha ao recarregar cache local.")
                             st.exception(reload_err)
                    # Error messages are handled within add_cliente_local_and_gsheet

# ==========================
# Tab 4: Atribuir Cliente-Colaborador
# ==========================
with tab4:
    st.subheader("Atribuir Clientes a Colaboradores")

    # --- Selecionar Colaborador ---
    colaboradores = manager.listar_colaboradores_local()
    if not colaboradores:
         st.warning("Nenhum colaborador ('Usuario') cadastrado para atribuir clientes.")
    else:
        colab_map = {c['nome_completo']: c['username'] for c in colaboradores}
        colab_names = ["Selecione..."] + sorted(list(colab_map.keys()))
        selected_colab_name = st.selectbox("Selecione o Colaborador:", colab_names, key="assign_colab_select")

        if selected_colab_name != "Selecione...":
            selected_colab_username = colab_map[selected_colab_name]
            st.write(f"Editando atribuições para: **{selected_colab_name}** (`{selected_colab_username}`)")

            # --- Listar Clientes Atribuídos e Disponíveis ---
            all_clients_local = manager.listar_clientes_local()
            all_client_names = sorted([c['nome'] for c in all_clients_local]) if all_clients_local else []

            assigned_clients = manager.get_assigned_clients_local(selected_colab_username)
            available_clients = sorted(list(set(all_client_names) - set(assigned_clients)))

            st.markdown("**Clientes Atualmente Atribuídos:**")
            if assigned_clients:
                st.multiselect("Clientes Atribuídos (selecione para remover):",
                               options=assigned_clients,
                               default=[], # Start with none selected for removal
                               key="unassign_clients_multi")
                if st.button("🗑️ Remover Atribuições Selecionadas"):
                    clients_to_remove = st.session_state.unassign_clients_multi
                    if not clients_to_remove:
                         st.warning("Nenhum cliente selecionado para remoção.")
                    else:
                         with st.spinner(f"Removendo atribuições de {selected_colab_name}..."):
                            unassign_success = manager.unassign_clients_from_collab(selected_colab_username, clients_to_remove)
                            if unassign_success:
                                 # Refresh local data to show changes
                                 try:
                                     manager.load_data_for_session(admin_username, admin_role)
                                     st.success("Atribuições removidas e cache atualizado.")
                                     st.rerun() # Rerun to update multiselects
                                 except Exception as e:
                                     st.error("Atribuições removidas, mas falha ao recarregar cache local.")
                            # Error displayed by manager method

            else:
                st.caption("Nenhum cliente atribuído a este colaborador.")

            st.markdown("**Clientes Disponíveis para Atribuição:**")
            if available_clients:
                st.multiselect("Clientes Disponíveis (selecione para adicionar):",
                               options=available_clients,
                               default=[],
                               key="assign_clients_multi")
                if st.button("➕ Adicionar Atribuições Selecionadas"):
                     clients_to_add = st.session_state.assign_clients_multi
                     if not clients_to_add:
                          st.warning("Nenhum cliente selecionado para adição.")
                     else:
                           with st.spinner(f"Adicionando atribuições para {selected_colab_name}..."):
                                assign_success = manager.assign_clients_to_collab(selected_colab_username, clients_to_add)
                                if assign_success:
                                      try:
                                          manager.load_data_for_session(admin_username, admin_role)
                                          st.success("Atribuições adicionadas e cache atualizado.")
                                          st.rerun() # Rerun to update multiselects
                                      except Exception as e:
                                          st.error("Atribuições adicionadas, mas falha ao recarregar cache local.")
                                # Error displayed by manager method

            else:
                 st.caption("Nenhum cliente disponível para novas atribuições.")

# Clearer separation for the form content ends here