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
tab1, tab2, tab3, tab4, tab5 = st.tabs([ # Added Tab 5 implicitly by creating the page
    "📊 Visão Documentos", # Renamed Tab 1
    "👤 Cadastrar Usuário",
    "🏢 Cadastrar Cliente",
    "🔗 Atribuir Cliente-Colaborador",
    "⚖️ Validar Documentos" # Explicitly referencing the new page
])

# ==========================
# Tab 1: Visão Documentos (Enhanced)
# ==========================
with tab1:
    st.subheader("Visão Detalhada de Todos os Documentos (Cache Local)")

    # --- Filters for Document View ---
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        # Filter by User
        colaboradores = manager.listar_colaboradores_local()
        colab_options_map = {"Todos": None}
        colab_options_map.update({c['nome_completo']: c['username'] for c in colaboradores})
        selected_colab_name_ov = st.selectbox("Filtrar por Colaborador:", list(colab_options_map.keys()), key="ov_colab_filter")
        user_filter_ov = colab_options_map[selected_colab_name_ov]
    with col_f2:
        # Filter by Client
        clientes_list_ov = manager.listar_clientes_local(colaborador_username=user_filter_ov)
        client_options_ov = ["Todos"] + sorted([c['nome'] for c in clientes_list_ov]) if clientes_list_ov else ["Todos"]
        selected_client_name_ov = st.selectbox(
            "Filtrar por Cliente:", client_options_ov, key="ov_client_filter",
            disabled=(user_filter_ov is not None and not clientes_list_ov)
        )
        client_filter_ov = selected_client_name_ov if selected_client_name_ov != "Todos" else None
    with col_f3:
        # Filter by Status
        status_options_ov = ["Todos"] + config.VALID_STATUSES
        selected_status_ov = st.selectbox("Filtrar por Status:", status_options_ov, key="ov_status_filter")
        status_filter_ov = selected_status_ov if selected_status_ov != "Todos" else None

    # --- Fetch and Display Data ---
    all_docs = manager.get_all_documents_local(
        status_filter=status_filter_ov,
        user_filter=user_filter_ov,
        client_filter=client_filter_ov
    )

    if all_docs:
        df_all_docs = pd.DataFrame(all_docs)

        # --- Format Dates and Select Columns ---
        def format_display_date(date_str, fmt="%d/%m/%Y"):
             if not date_str or pd.isna(date_str): return ""
             try: return pd.to_datetime(date_str).strftime(fmt)
             except: return str(date_str) # Fallback

        def format_display_datetime(dt_str, fmt="%d/%m/%Y %H:%M"):
             if not dt_str or pd.isna(dt_str): return ""
             try: return pd.to_datetime(dt_str).strftime(fmt)
             except: return str(dt_str) # Fallback

        df_display_ov = df_all_docs.copy()
        if 'data_registro' in df_display_ov.columns:
            df_display_ov['data_registro'] = df_display_ov['data_registro'].apply(format_display_date)
        if 'data_validacao' in df_display_ov.columns:
            df_display_ov['data_validacao'] = df_display_ov['data_validacao'].apply(format_display_datetime)

        # Define desired columns for the overview table
        overview_cols = [
            'colaborador_username', 'cliente_nome', 'data_registro', 'status',
            'dimensao_criterio', 'link_ou_documento', 'quantidade',
            'data_validacao', 'validado_por', 'observacoes_validacao', 'id'
        ]
        overview_cols = [col for col in overview_cols if col in df_display_ov.columns] # Ensure cols exist

        st.dataframe(df_display_ov[overview_cols], use_container_width=True, hide_index=True)
        st.info(f"Total de documentos no cache local (com filtros aplicados): {len(df_display_ov)}")
    else:
        st.warning("Nenhum documento encontrado no cache local com os filtros selecionados.")

    st.divider()
    st.subheader("Ações Gerais")
    if st.button("🔄 Recarregar Todos os Dados das Planilhas", key="reload_all_data_tab1"):
         with st.spinner("Recarregando todos os dados..."):
            try:
                # Clear local cache before reloading? Might be good.
                # manager._execute_local_sql("DELETE FROM documentos")
                # manager._execute_local_sql("DELETE FROM usuarios")
                # etc. Or just rely on load_data_for_session's replace logic.
                manager.load_data_for_session(admin_username, admin_role)
                st.success("Dados recarregados com sucesso!")
                st.rerun()
            except Exception as e:
                st.error("Falha ao recarregar os dados.")
                st.exception(e)

    # --- Keep KPIs and Pontuação Calculation ---
    st.divider()
    st.subheader("KPIs Gerais (Todos Usuários - Cache Local)")
    kpi_geral = manager.get_kpi_data_local() # Uses local cache
    col1, col2, col3, col4 = st.columns(4)
    # Adjust KPI names if needed based on how you map statuses
    col1.metric("Docs Cadastrados", f"{kpi_geral.get('docs_enviados', 0)}") # Example: Combine Enviado+Pendente
    col2.metric("Docs Validados", f"{kpi_geral.get('docs_validados', 0):02d}")
    col3.metric("Docs Inválidos", f"{kpi_geral.get('docs_invalidos', 0):02d}")
    # col4.metric("Algum outro KPI?", ...)
    st.divider()

    st.subheader("Pontuação Atualizada (Direto das Planilhas)")
    if st.button("📊 Calcular Pontuação Atualizada (Pode ser Lento)"):
        with st.spinner("Buscando dados e calculando pontuação das planilhas..."):
            manager.calcular_pontuacao_colaboradores_gsheet.clear() # Clear cache for this specific function
            df_pontuacao_gsheet = manager.calcular_pontuacao_colaboradores_gsheet() # Calls GSheet directly
        if not df_pontuacao_gsheet.empty:
            st.dataframe(df_pontuacao_gsheet)
        else:
            st.warning("Não foi possível calcular a pontuação diretamente das planilhas ou não há dados.")


# ==========================
# Tab 2: Cadastrar Usuário
# ==========================
with tab2:
    # ... (Keep existing user registration logic) ...
    st.subheader("Cadastrar Novo Usuário no Sistema")
    with st.form("new_user_form", clear_on_submit=True):
        # ... form fields ...
        new_username = st.text_input("Nome de Usuário (Login)", key="nu_uname").strip()
        new_fullname = st.text_input("Nome Completo", key="nu_fname").strip()
        new_password = st.text_input("Senha Temporária", type="password", key="nu_pass")
        new_role = st.selectbox("Perfil (Role)", ["Usuario", "Admin", "Cliente"], key="nu_role")
        submitted = st.form_submit_button("✨ Cadastrar Usuário")
        if submitted:
             # ... validation and registration logic ...
            if not all([new_username, new_fullname, new_password, new_role]):
                st.error("❌ Por favor, preencha todos os campos.")
            else:
                with st.spinner(f"Verificando e cadastrando '{new_username}'..."):
                    # ... (duplicate check logic) ...
                    is_duplicate = False # Placeholder
                    try:
                         users_ws_check = manager._get_worksheet(config.SHEET_USERS)
                         if users_ws_check:
                              cell = users_ws_check.find(new_username, in_column=config.USERS_COLS.index('username') + 1)
                              if cell: is_duplicate = True
                         else: raise Exception("Planilha de usuários não encontrada.")
                    except Exception as find_err:
                         st.error(f"Erro ao verificar duplicidade: {find_err}")
                         st.stop()

                    if is_duplicate:
                         st.error(f"❌ Erro: Nome de usuário '{new_username}' já existe!")
                    else:
                         # ... (hashing, appending user to sheet logic) ...
                         hashed_pw = manager._hash_password(new_password)
                         user_data_list = [new_username, hashed_pw, new_fullname, new_role, None]
                         user_data_to_append = user_data_list[:len(config.USERS_COLS)]
                         user_added_success = False
                         try:
                              users_ws = manager._get_worksheet(config.SHEET_USERS) # Get it again just in case
                              if not users_ws: raise Exception("Planilha de usuários não encontrada para adicionar.")
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
                                   existing_ws = manager.spreadsheet.worksheet(docs_sheet_name)
                                   st.warning(f"⚠️ Planilha '{docs_sheet_name}' já existe. Não será recriada.")
                                   sheet_created_or_not_needed = True
                              except gspread.exceptions.WorksheetNotFound:
                                   # Expected case: Sheet doesn't exist, try to create
                                   new_ws = manager.spreadsheet.add_worksheet(
                                        title=docs_sheet_name,
                                        rows=20, # Start with reasonable rows
                                        cols=len(config.DOCS_COLS) # <<< CORRECT: Uses length of updated list
                                   )
                                   # Add header row immediately using the updated list
                                   new_ws.update([config.DOCS_COLS], value_input_option='USER_ENTERED') # <<< CORRECT: Writes updated header
                                   st.success(f"✅ Planilha '{docs_sheet_name}' criada com cabeçalho completo.")
                                   sheet_created_or_not_needed = True

                         # ... (reloading local data logic) ...
                         if user_added_success:
                              st.info("Atualizando cache de dados local...")
                              try:
                                   manager.load_data_for_session(admin_username, admin_role)
                                   st.success("Cache local atualizado.")
                              except Exception as reload_err:
                                   st.error("Usuário adicionado, mas falha ao recarregar cache local.")


# ==========================
# Tab 3: Cadastrar Cliente
# ==========================
with tab3:
    # ... (Keep existing client registration logic) ...
    st.subheader("Cadastrar Novo Cliente no Sistema")
    with st.form("new_client_form", clear_on_submit=True):
        # ... form fields ...
        new_client_name = st.text_input("Nome do Cliente", key="nc_name").strip()
        tipos_existentes = list(set([c['tipo'] for c in manager.listar_clientes_local() if c['tipo']]))
        tipos_opcao = sorted(list(set(["Prefeitura", "Câmara", "Autarquia", "Outro"] + tipos_existentes)))
        new_client_type = st.selectbox("Tipo de Cliente", tipos_opcao, key="nc_type")
        submit_new_client = st.form_submit_button("🏢 Cadastrar Cliente")
        if submit_new_client:
             # ... validation ...
             if not new_client_name or not new_client_type:
                 st.error("❌ Por favor, preencha todos os campos.")
             else:
                with st.spinner(f"Cadastrando cliente '{new_client_name}'..."):
                    success = manager.add_cliente_local_and_gsheet(new_client_name, new_client_type)
                    if success:
                        # ... reload local data ...
                        try:
                             manager.load_data_for_session(admin_username, admin_role)
                             st.success("Cache local atualizado.")
                        except Exception as reload_err:
                             st.error("Cliente adicionado, mas falha ao recarregar cache local.")


# ==========================
# Tab 4: Atribuir Cliente-Colaborador
# ==========================
with tab4:
    # ... (Keep existing assignment logic) ...
    st.subheader("Atribuir Clientes a Colaboradores")
    # ... collaborator selection ...
    colaboradores_assign = manager.listar_colaboradores_local()
    if not colaboradores_assign:
         st.warning("Nenhum colaborador ('Usuario') cadastrado.")
    else:
        colab_map_assign = {c['nome_completo']: c['username'] for c in colaboradores_assign}
        colab_names_assign = ["Selecione..."] + sorted(list(colab_map_assign.keys()))
        selected_colab_name_assign = st.selectbox("Selecione o Colaborador:", colab_names_assign, key="assign_colab_select")

        if selected_colab_name_assign != "Selecione...":
            selected_colab_username_assign = colab_map_assign[selected_colab_name_assign]
            st.write(f"Editando atribuições para: **{selected_colab_name_assign}**")

            # ... listing assigned and available clients ...
            all_clients_local_assign = manager.listar_clientes_local()
            all_client_names_assign = sorted([c['nome'] for c in all_clients_local_assign]) if all_clients_local_assign else []
            assigned_clients_assign = manager.get_assigned_clients_local(selected_colab_username_assign)
            available_clients_assign = sorted(list(set(all_client_names_assign) - set(assigned_clients_assign)))

            # ... multiselect for removing ...
            st.multiselect("Remover Atribuições:", assigned_clients_assign, key="unassign_clients_multi")
            if st.button("🗑️ Remover Selecionadas"):
                 # ... unassign logic ...
                 clients_to_remove = st.session_state.unassign_clients_multi
                 if clients_to_remove:
                    with st.spinner("Removendo..."):
                        unassign_success = manager.unassign_clients_from_collab(selected_colab_username_assign, clients_to_remove)
                        if unassign_success:
                            try:
                                manager.load_data_for_session(admin_username, admin_role)
                                st.success("Removido e cache atualizado.")
                                st.rerun()
                            except Exception as e:
                                st.error("Removido, mas falha ao recarregar cache.")
                 else: st.warning("Nenhum cliente selecionado.")


            # ... multiselect for adding ...
            st.multiselect("Adicionar Atribuições:", available_clients_assign, key="assign_clients_multi")
            if st.button("➕ Adicionar Selecionadas"):
                # ... assign logic ...
                clients_to_add = st.session_state.assign_clients_multi
                if clients_to_add:
                    with st.spinner("Adicionando..."):
                         assign_success = manager.assign_clients_to_collab(selected_colab_username_assign, clients_to_add)
                         if assign_success:
                              try:
                                   manager.load_data_for_session(admin_username, admin_role)
                                   st.success("Adicionado e cache atualizado.")
                                   st.rerun()
                              except Exception as e:
                                   st.error("Adicionado, mas falha ao recarregar cache.")
                else: st.warning("Nenhum cliente selecionado.")

with tab5: 
     # --- Check Login and Role ---
    if not st.session_state.get('data_loaded') or not st.session_state.get('db_manager'):
        st.warning("Os dados ainda estão sendo carregados ou o gerenciador não foi inicializado.")
        st.stop()

    manager = st.session_state.db_manager
    admin_username = st.session_state.get('username')

    # --- Page Title ---
    st.subheader("Validação de Documentos")
    st.divider()

    # --- Filters ---
    st.sidebar.header("Filtros de Validação")

    # Filter by User
    colaboradores = manager.listar_colaboradores_local()
    colab_options_map = {"Todos": None}
    colab_options_map.update({c['nome_completo']: c['username'] for c in colaboradores})
    selected_colab_name = st.sidebar.selectbox("Filtrar por Colaborador:", list(colab_options_map.keys()), key="val_colab_filter")
    selected_colab_filter_user = colab_options_map[selected_colab_name]

    # Filter by Client
    # Get clients relevant to the selected collaborator (or all if 'Todos' selected)
    clientes_list = manager.listar_clientes_local(colaborador_username=selected_colab_filter_user)
    client_options_names_only = ["Todos"] + sorted([c['nome'] for c in clientes_list]) if clientes_list else ["Todos"]
    selected_client_name_filter = st.sidebar.selectbox(
        "Filtrar por Cliente:",
        client_options_names_only,
        key="val_client_filter",
        disabled=(selected_colab_filter_user is not None and not clientes_list) # Disable if specific user has no clients
    )
    client_filter_val = selected_client_name_filter if selected_client_name_filter != "Todos" else None


    # Filter by Status
    status_options = ["Todos"] + config.VALID_STATUSES
    # Default to showing statuses that typically need validation
    default_statuses_to_show = ['Cadastrado', 'Inválido']
    selected_status_filter = st.sidebar.multiselect(
        "Filtrar por Status:",
        options=status_options,
        default=default_statuses_to_show,
        key="val_status_filter"
    )
    status_filter_val = selected_status_filter if "Todos" not in selected_status_filter else None


    # --- Fetch Data based on Filters ---
    docs_to_validate = manager.get_all_documents_local(
        status_filter=None, # Apply multi-select filter after fetching based on user/client
        user_filter=selected_colab_filter_user,
        client_filter=client_filter_val
    )

    df_docs = pd.DataFrame(docs_to_validate)

    # Apply multi-status filter locally if needed
    if status_filter_val:
          if not df_docs.empty and 'status' in df_docs.columns:
               df_docs = df_docs[df_docs['status'].isin(status_filter_val)]
          elif 'status' not in df_docs.columns and not df_docs.empty:
               # Warn if the column is missing but there is data
               st.warning("A coluna 'status' não foi encontrada nos dados carregados. O filtro de status não pode ser aplicado.")
          # If df_docs is empty, do nothing, the check below will handle it.



    # --- Display Data for Validation using Data Editor ---
    if not df_docs.empty:
        st.info(f"Exibindo {len(df_docs)} documentos para validação.")

        # Prepare DataFrame for editor
        df_display = df_docs.copy()

        # Add columns needed for interaction in the editor
        df_display['Marcar para Validar'] = False
        df_display['Novo Status'] = df_display['status'] # Initialize with current status
        df_display['Observações'] = df_display['observacoes_validacao'].fillna('') # Use existing or empty

        # Define columns to show and their order/configuration
        cols_to_show_editor = [
            'Marcar para Validar',
            'Novo Status',
            'Observações',
            'link_ou_documento',
            'status', # Show current status for reference
            'colaborador_username',
            'cliente_nome',
            'data_registro',
            'dimensao_criterio',
            'id', # Keep ID for reference, but disable editing
            'data_validacao',
            'validado_por'
        ]
        # Filter out columns that might not exist in the dataframe yet
        cols_to_show_editor = [col for col in cols_to_show_editor if col in df_display.columns]

        # Configure editor columns
        column_config = {
            "Marcar para Validar": st.column_config.CheckboxColumn(required=True),
            "Novo Status": st.column_config.SelectboxColumn(
                "Novo Status",
                help="Selecione o novo status para aplicar.",
                options=config.VALID_STATUSES,
                required=True
            ),
            "Observações": st.column_config.TextColumn(
                "Observações",
                help="Adicione comentários sobre a validação (opcional).",
                width="medium"
            ),
            "link_ou_documento": st.column_config.LinkColumn( # Make link clickable
                "Link/Documento",
                help="Clique para abrir o link",
                width="large",
                display_text="Abrir/Ver" # Customize text? Or show URL? Show URL is better.
            ),
            "status": st.column_config.TextColumn("Status Atual", disabled=True),
            "colaborador_username": st.column_config.TextColumn("Colaborador", disabled=True),
            "cliente_nome": st.column_config.TextColumn("Cliente", disabled=True),
            "data_registro": st.column_config.DateColumn("Data Reg.", format="DD/MM/YYYY", disabled=True),
            "dimensao_criterio": st.column_config.TextColumn("Critério", disabled=True),
            "id": st.column_config.TextColumn("ID", disabled=True, width="small"),
            "data_validacao": st.column_config.DatetimeColumn("Data Validação", format="DD/MM/YYYY HH:mm", disabled=True),
            "validado_por": st.column_config.TextColumn("Validado Por", disabled=True),
        }

        # Ensure config only includes columns present in the dataframe
        final_column_config = {k: v for k, v in column_config.items() if k in cols_to_show_editor}


        st.markdown("Marque os documentos, selecione o **Novo Status**, adicione observações (opcional) e clique em 'Processar Validações'.")

        # Use a unique key for the editor to help with state management
        if 'validation_editor_key' not in st.session_state:
            st.session_state.validation_editor_key = 0

        edited_df = st.data_editor(
            df_display[cols_to_show_editor], # Show only selected columns
            column_config=final_column_config,
            key=f"validation_editor_{st.session_state.validation_editor_key}",
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic" # Adjust height automatically
        )

        # --- Process Validation Button ---
        st.divider()
        marked_rows = edited_df[edited_df['Marcar para Validar'] == True]
        num_marked = len(marked_rows)

        if st.button(f"🚀 Processar {num_marked} Validações Marcadas", disabled=(num_marked == 0)):
            if num_marked > 0:
                success_count = 0
                fail_count = 0
                with st.spinner("Processando validações e atualizando planilhas..."):
                    for index, row in marked_rows.iterrows():
                        doc_id = row['id']
                        new_status = row['Novo Status']
                        observacoes = row['Observações']

                        # Call the manager method to update GSheet and Local DB
                        update_success = manager.update_document_status_gsheet_and_local(
                            doc_id=doc_id,
                            new_status=new_status,
                            admin_username=admin_username, # Logged-in admin
                            observacoes=observacoes
                        )
                        if update_success:
                            success_count += 1
                        else:
                            fail_count += 1
                            st.warning(f"Falha ao processar ID: {doc_id}") # Show immediate feedback on failure

                st.toast(f"Processamento concluído!")
                if success_count > 0:
                    st.success(f"{success_count} documentos validados/atualizados com sucesso!")
                if fail_count > 0:
                    st.error(f"{fail_count} validações falharam. Verifique os avisos acima e a planilha.")

                # Increment key to force re-render of the data editor with fresh data
                st.session_state.validation_editor_key += 1
                st.rerun() # Rerun to reflect changes immediately

    else:
        st.info("Nenhum documento encontrado com os filtros selecionados.")

# Tab 5 is implicitly handled by the existence of pages/4_Validação_Documentos.py
# Streamlit automatically adds it to the navigation based on filename.