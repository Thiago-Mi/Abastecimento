import streamlit as st
import pandas as pd
from datetime import datetime
import config
import gspread 

st.set_page_config(layout="wide")

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

st.markdown("#### 👑 Painel de Administração")
st.divider()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Visão Documentos", 
    "👤 Cadastrar Usuário",
    "🏢 Cadastrar Cliente",
    "🔗 Atribuir Cliente-Colaborador",
    "⚖️ Validar Documentos" 
])

with tab1: # Visão Documentos
    st.subheader("Visão Detalhada de Todos os Documentos")
    col_f1, col_f2, col_f3, col_f4 = st.columns(4) # Added column for Tipo Cliente
    
    with col_f1:
        colaboradores = manager.listar_colaboradores_local()
        colab_options_map = {"Todos": None}
        colab_options_map.update({c['nome_completo']: c['username'] for c in colaboradores})
        selected_colab_name_ov = st.selectbox("Filtrar por Colaborador:", list(colab_options_map.keys()), key="ov_colab_filter")
        user_filter_ov = colab_options_map[selected_colab_name_ov]

    # Get all client types for the filter
    all_clients_for_types = manager.listar_clientes_local() # Get all clients for types
    available_client_types_ov = sorted(list(set(c['tipo'] for c in all_clients_for_types if c['tipo'])))

    with col_f2: # Tipo Cliente Filter
        selected_tipos_ov = ["Todos"]
        if available_client_types_ov:
            selected_tipos_ov = st.multiselect(
                "Filtrar por Tipo de Cliente:",
                options=available_client_types_ov,
                key="ov_tipo_cliente_filter"
            )
            if not selected_tipos_ov: selected_tipos_ov = ["Todos"] # Default to all if none selected
    
    with col_f3: # Filter by Client (now depends on selected types)
        # Get clients filtered by selected types (if any)
        tipos_to_pass_to_manager = selected_tipos_ov if "Todos" not in selected_tipos_ov else None
        clientes_list_ov_dicts = manager.listar_clientes_local(
            colaborador_username=user_filter_ov, # Retains original collaborator filter if any
            tipos_filter=tipos_to_pass_to_manager
        )
        client_options_ov_map = {"Todos": None} # name: id
        if clientes_list_ov_dicts:
            client_options_ov_map.update({c['nome']: c['id'] for c in clientes_list_ov_dicts})

        selected_client_name_ov = st.selectbox(
            "Filtrar por Cliente:", list(client_options_ov_map.keys()), key="ov_client_filter",
            disabled=(user_filter_ov is not None and not clientes_list_ov_dicts and "Todos" not in selected_tipos_ov)
        )
        client_id_filter_ov = client_options_ov_map.get(selected_client_name_ov) # Get ID

    with col_f4: # Filter by Status
        status_options_ov = ["Todos"] + config.VALID_STATUSES
        selected_status_ov = st.selectbox("Filtrar por Status:", status_options_ov, key="ov_status_filter")
        status_filter_ov = selected_status_ov if selected_status_ov != "Todos" else None

    all_docs = manager.get_all_documents_local(
        status_filter=status_filter_ov,
        user_filter=user_filter_ov,
        cliente_id_filter=client_id_filter_ov, # Pass ID
        tipos_cliente_filter=selected_tipos_ov if "Todos" not in selected_tipos_ov else None
    )

    if all_docs:
        df_all_docs = pd.DataFrame(all_docs)
        def format_display_date(date_str, fmt="%d/%m/%Y"):
             if not date_str or pd.isna(date_str): return ""
             try: return pd.to_datetime(date_str).strftime(fmt)
             except: return str(date_str)
        def format_display_datetime(dt_str, fmt="%d/%m/%Y %H:%M"):
             if not dt_str or pd.isna(dt_str): return ""
             try: return pd.to_datetime(dt_str).strftime(fmt)
             except: return str(dt_str) 
        df_display_ov = df_all_docs.copy()
        if 'data_registro' in df_display_ov.columns: df_display_ov['data_registro'] = df_display_ov['data_registro'].apply(format_display_date)
        if 'data_validacao' in df_display_ov.columns: df_display_ov['data_validacao'] = df_display_ov['data_validacao'].apply(format_display_datetime)
        # print(df_display_ov.columns,'##################################################################################################')
        # Use 'nome_cliente_join' and 'tipo_cliente' from get_all_documents_local if needed for display
        # However, DOCS_COLS still has 'cliente_nome' which should be populated
        overview_cols = ['colaborador_username', 'cliente_nome', 'tipo_cliente','data_registro', 'status',
                         'dimensao_criterio', 'link_ou_documento',
                         'data_validacao', 'validado_por', 'observacoes_validacao']
        overview_cols = [col for col in overview_cols if col in df_display_ov.columns]
        st.dataframe(df_display_ov[overview_cols], use_container_width=True, hide_index=True, height=600)
        st.info(f"Total de documentos no cache local (filtros aplicados): {len(df_display_ov)}")
    else:
        st.warning("Nenhum documento encontrado no cache local com os filtros selecionados.")
    st.divider()
    st.subheader("Ações Gerais")
    if st.button("🔄 Recarregar Todos os Dados das Planilhas", key="reload_all_data_tab1"):
         with st.spinner("Recarregando todos os dados..."):
            try:
                manager.load_data_for_session(admin_username, admin_role)
                st.success("Dados recarregados com sucesso!")
                st.rerun()
            except Exception as e: st.error(f"Falha ao recarregar os dados: {e}")
    st.divider()
    st.subheader("KPIs Gerais (Todos Usuários - Cache Local)")
    # KPIs can also be filtered by client type if desired, by passing selected_tipos_ov
    kpi_geral_admin_tab = manager.get_kpi_data_local(
        tipos_cliente_filter=selected_tipos_ov if "Todos" not in selected_tipos_ov else None
    )
    col1, col2, col3 = st.columns(3)
    col1.metric("Docs Cadastrados", f"{kpi_geral_admin_tab.get('docs_enviados', 0)}")
    col2.metric("Docs Validados", f"{kpi_geral_admin_tab.get('docs_validados', 0):02d}")
    col3.metric("Docs Inválidos", f"{kpi_geral_admin_tab.get('docs_invalidos', 0):02d}")
    st.divider()
    st.subheader("Pontuação Atualizada (Direto das Planilhas)")
    if st.button("📊 Calcular Pontuação Atualizada (Pode ser Lento)"):
        with st.spinner("Buscando dados e calculando pontuação das planilhas..."):
            manager.calcular_pontuacao_colaboradores_gsheet.clear() 
            df_pontuacao_gsheet = manager.calcular_pontuacao_colaboradores_gsheet()
        if not df_pontuacao_gsheet.empty: st.dataframe(df_pontuacao_gsheet)
        else: st.warning("Não foi possível calcular a pontuação diretamente das planilhas ou não há dados.")

# Tab 2: Cadastrar Usuário (No changes for client type directly)
with tab2:
    st.subheader("Cadastrar Novo Usuário no Sistema")
    with st.form("new_user_form", clear_on_submit=True):
        new_username = st.text_input("Nome de Usuário (Login)", key="nu_uname").strip()
        new_fullname = st.text_input("Nome Completo", key="nu_fname").strip()
        new_password = st.text_input("Senha Temporária", type="password", key="nu_pass")
        new_role = st.selectbox("Perfil (Role)", ["Usuario", "Admin", "Cliente"], key="nu_role")
        # If role is Cliente, ensure username matches an existing client name for simplicity.
        # Or add a dropdown to select an existing client to be the user.
        # Current logic: Client username IS the client name.
        submitted = st.form_submit_button("✨ Cadastrar Usuário")
        if submitted:
            if not all([new_username, new_fullname, new_password, new_role]):
                st.error("❌ Por favor, preencha todos os campos.")
            else:
                # Additional check if role is 'Cliente'
                if new_role == 'Cliente':
                    client_exists_check = manager.buscar_usuario_local(new_username) # Check if username already exists (any role)
                    if not client_exists_check: # If user doesn't exist, check if a client with this name exists
                        client_record = manager._execute_local_sql("SELECT id FROM clientes WHERE nome = ? COLLATE NOCASE", (new_username,), fetch_mode="one")
                        if not client_record:
                            st.error(f"❌ Para perfil 'Cliente', o nome de usuário ('{new_username}') deve corresponder a um nome de Cliente já cadastrado. Cadastre o cliente primeiro.")
                            st.stop()
                    # else: proceed, username already exists or will be checked below.

                with st.spinner(f"Verificando e cadastrando '{new_username}'..."):
                    is_duplicate = False 
                    try:
                         users_ws_check = manager._get_worksheet(config.SHEET_USERS)
                         if users_ws_check:
                              all_users_records = users_ws_check.get_all_records() # More reliable than find() for partial matches
                              if any(str(r.get('username','')).lower() == new_username.lower() for r in all_users_records):
                                  is_duplicate = True
                         else: raise Exception("Planilha de usuários não encontrada.")
                    except Exception as find_err:
                         st.error(f"Erro ao verificar duplicidade: {find_err}"); st.stop()
                    if is_duplicate:
                         st.error(f"❌ Erro: Nome de usuário '{new_username}' já existe!")
                    else:
                         hashed_pw = manager._hash_password(new_password)
                         user_data_list = [new_username, hashed_pw, new_fullname, new_role, None] 
                         user_data_to_append = user_data_list[:len(config.USERS_COLS)]
                         user_added_success = False
                         try:
                              users_ws = manager._get_worksheet(config.SHEET_USERS) 
                              if not users_ws: raise Exception("Planilha de usuários não encontrada para adicionar.")
                              users_ws.append_row(user_data_to_append, value_input_option='USER_ENTERED')
                              st.success(f"✅ Usuário '{new_username}' ({new_role}) adicionado à planilha principal.")
                              user_added_success = True
                         except Exception as append_err:
                              st.error(f"❌ Falha ao adicionar usuário '{new_username}' na planilha: {append_err}")
                         if user_added_success and new_role == 'Usuario':
                              docs_sheet_name = manager._get_user_sheet_name(new_username)
                              st.write(f"Tentando criar planilha de documentos '{docs_sheet_name}'...")
                              try:
                                   existing_ws = manager.spreadsheet.worksheet(docs_sheet_name)
                                   st.warning(f"⚠️ Planilha '{docs_sheet_name}' já existe. Não será recriada.")
                              except gspread.exceptions.WorksheetNotFound:
                                   new_ws = manager.spreadsheet.add_worksheet(title=docs_sheet_name, rows=20, cols=len(config.DOCS_COLS))
                                   new_ws.update([config.DOCS_COLS], value_input_option='USER_ENTERED') 
                                   st.success(f"✅ Planilha '{docs_sheet_name}' criada com cabeçalho completo.")
                         if user_added_success:
                              st.info("Atualizando cache de dados local...")
                              try:
                                   manager.load_data_for_session(admin_username, admin_role)
                                   st.success("Cache local atualizado.")
                              except Exception: st.error("Usuário adicionado, mas falha ao recarregar cache local.")

# Tab 3: Cadastrar Cliente (already handles 'tipo')
with tab3:
    st.subheader("Cadastrar Novo Cliente no Sistema")
    with st.form("new_client_form", clear_on_submit=True):
        new_client_name = st.text_input("Nome do Cliente", key="nc_name").strip()
        # Get existing types for better suggestions
        current_clients_tab3 = manager.listar_clientes_local()
        tipos_existentes = sorted(list(set([c['tipo'] for c in current_clients_tab3 if c['tipo']])))
        tipos_opcao = sorted(list(set(["Prefeitura", "Câmara", "Autarquia", "Outro"] + tipos_existentes)))
        
        new_client_type = st.selectbox("Tipo de Cliente", tipos_opcao, key="nc_type", index=0 if "Prefeitura" in tipos_opcao else 0)
        custom_type = st.text_input("Ou Especifique Outro Tipo:", key="nc_custom_type").strip()

        submit_new_client = st.form_submit_button("🏢 Cadastrar Cliente")
        if submit_new_client:
             final_client_type = custom_type if new_client_type == "Outro" and custom_type else new_client_type
             if not new_client_name or not final_client_type or final_client_type == "Outro": # Ensure type is specified
                 st.error("❌ Por favor, preencha Nome do Cliente e selecione/especifique um Tipo de Cliente válido.")
             else:
                with st.spinner(f"Cadastrando cliente '{new_client_name}' ({final_client_type})..."):
                    success = manager.add_cliente_local_and_gsheet(new_client_name, final_client_type)
                    if success:
                        try:
                             manager.load_data_for_session(admin_username, admin_role) # Reload to get new client
                             st.success("Cache local atualizado.")
                             st.rerun() # Rerun to refresh selectbox options if needed
                        except Exception: st.error("Cliente adicionado, mas falha ao recarregar cache local.")
                    # Else, add_cliente_local_and_gsheet already showed an error

# Tab 4: Atribuir Cliente-Colaborador (No direct change for client type filter here, but uses latest client list)
with tab4:
    st.subheader("Atribuir Clientes a Colaboradores")
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

            # --- Obter TODOS os clientes (lista de dicts com id, nome, tipo) ---
            all_clients_list_of_dicts = manager.listar_clientes_local()
            
            # Criar um mapa de ID para string de exibição para o format_func
            client_id_to_display_map = {
                client['id']: f"{client['nome']} (Tipo: {client['tipo']})"
                for client in all_clients_list_of_dicts
            }
            def format_client_for_multiselect(client_id):
                return client_id_to_display_map.get(client_id, f"ID Desconhecido: {client_id}")

            # --- Obter clientes JÁ ATRIBUÍDOS (lista de dicts com id, nome, tipo) ---
            assigned_clients_info_list = manager.get_assigned_clients_local(selected_colab_username_assign)
            assigned_client_ids = {client['id'] for client in assigned_clients_info_list}


            # --- Filtro de Tipo para Clientes DISPONÍVEIS ---
            all_client_types_assign = sorted(list(set(c['tipo'] for c in all_clients_list_of_dicts if c['tipo'])))
            selected_type_filter_assign = "Todos"
            if all_client_types_assign:
                filter_options_assign = ["Todos"] + all_client_types_assign
                selected_type_filter_assign = st.selectbox(
                    "Filtrar Clientes Disponíveis por Tipo:",
                    options=filter_options_assign,
                    key="assign_tipo_cliente_filter_tab4" # Chave única
                )

            # --- Preparar lista de clientes DISPONÍVEIS (IDs) ---
            available_clients_options_ids = []
            for client_dict in all_clients_list_of_dicts:
                # Incluir se não estiver já atribuído E corresponder ao filtro de tipo (se não for "Todos")
                if client_dict['id'] not in assigned_client_ids:
                    if selected_type_filter_assign == "Todos" or client_dict['tipo'] == selected_type_filter_assign:
                        available_clients_options_ids.append(client_dict['id'])
            
            # Ordenar os IDs disponíveis pela string de exibição para consistência
            available_clients_options_ids.sort(key=lambda id_val: format_client_for_multiselect(id_val))


            # --- Widgets de Atribuição/Remoção ---
            st.markdown("---")
            st.write("**Clientes Atualmente Atribuídos:**")
            # Para remover, as opções são os IDs dos clientes já atribuídos
            options_for_removal_ids = [client['id'] for client in assigned_clients_info_list]
            options_for_removal_ids.sort(key=lambda id_val: format_client_for_multiselect(id_val))

            selected_ids_to_remove = st.multiselect(
                "Remover Atribuições:", 
                options=options_for_removal_ids, # Passa lista de IDs
                format_func=format_client_for_multiselect, # Exibe "Nome (Tipo)"
                key="unassign_clients_multi_ids", 
                label_visibility="collapsed"
            )
            if st.button("🗑️ Remover Selecionadas"):
                 if selected_ids_to_remove:
                    with st.spinner("Removendo..."):
                        # Passar selected_ids_to_remove para o manager
                        unassign_success = manager.unassign_clients_from_collab(selected_colab_username_assign, selected_ids_to_remove)
                        if unassign_success:
                            try: 
                                manager.load_data_for_session(admin_username, admin_role)
                                st.success("Removido e cache atualizado.")
                                st.rerun()
                            except Exception: st.error("Removido, mas falha ao recarregar cache.")
                 else: st.warning("Nenhum cliente selecionado para remover.")

            st.markdown("---")
            st.write(f"**Clientes Disponíveis (Tipo: {selected_type_filter_assign}):**")
            # Para adicionar, as opções são os IDs dos clientes disponíveis (já filtrados por tipo)
            selected_ids_to_add = st.multiselect(
                "Adicionar Atribuições:", 
                options=available_clients_options_ids, # Passa lista de IDs
                format_func=format_client_for_multiselect, # Exibe "Nome (Tipo)"
                key="assign_clients_multi_ids", 
                label_visibility="collapsed"
            )
            if st.button("➕ Adicionar Selecionadas"):
                if selected_ids_to_add:
                    with st.spinner("Adicionando..."):
                         # Passar selected_ids_to_add para o manager
                         assign_success = manager.assign_clients_to_collab(selected_colab_username_assign, selected_ids_to_add)
                         if assign_success:
                              try: 
                                  manager.load_data_for_session(admin_username, admin_role)
                                  st.success("Adicionado e cache atualizado.")
                                  st.rerun()
                              except Exception: st.error("Adicionado, mas falha ao recarregar cache.")
                else: st.warning("Nenhum cliente selecionado para adicionar.")

# Tab 5: Validar Documentos
with tab5: 
    st.subheader("Validação de Documentos")
    st.divider()
    st.header("Filtros de Validação")
    col_1, col_2, col_3, col_4 = st.columns(4)
    colaboradores_val = manager.listar_colaboradores_local()
    colab_options_map_val = {"Todos": None}
    colab_options_map_val.update({c['nome_completo']: c['username'] for c in colaboradores_val})
    with col_1:
        selected_colab_name_val = st.selectbox("Colaborador (Validação):", list(colab_options_map_val.keys()), key="val_colab_filter")
    selected_colab_filter_user_val = colab_options_map_val[selected_colab_name_val]

    # Filter by Client Type for Validation Tab
    all_clients_val_tab = manager.listar_clientes_local()
    available_client_types_val = sorted(list(set(c['tipo'] for c in all_clients_val_tab if c['tipo'])))
    
    selected_tipos_val = ["Todos"]
    if available_client_types_val:
        with col_2:
            selected_tipos_val = st.multiselect(
                "Tipo de Cliente (Validação):",
                options=available_client_types_val,
                key="val_tipo_cliente_filter"
            )
        if not selected_tipos_val: selected_tipos_val = ["Todos"]

    # Filter by Client (depends on selected type)
    tipos_to_pass_manager_val = selected_tipos_val if "Todos" not in selected_tipos_val else None
    clientes_list_val_dicts = manager.listar_clientes_local(
        colaborador_username=selected_colab_filter_user_val, # Optional: filter clients by who is assigned to them
        tipos_filter=tipos_to_pass_manager_val
    )
    client_options_val_map = {"Todos": None} # name: id
    if clientes_list_val_dicts:
        client_options_val_map.update({c['nome']: c['id'] for c in clientes_list_val_dicts})
    with col_3:
        selected_client_name_val = st.selectbox(
            "Cliente (Validação):",
            list(client_options_val_map.keys()),
            key="val_client_filter",
            disabled=(selected_colab_filter_user_val is not None and not clientes_list_val_dicts and "Todos" not in selected_tipos_val)
        )
    client_id_filter_val = client_options_val_map.get(selected_client_name_val) # Get ID

    status_options_val = ["Todos"] + config.VALID_STATUSES
    default_statuses_to_show_val = ['Cadastrado', 'Inválido']
    with col_4:
        selected_status_filter_val = st.multiselect(
            "Status (Validação):", options=status_options_val, default=default_statuses_to_show_val, key="val_status_filter"
        )
    status_filter_to_pass_val = selected_status_filter_val if "Todos" not in selected_status_filter_val and selected_status_filter_val else None

    docs_to_validate = manager.get_all_documents_local(
        status_filter=None, # Will apply multiselect status filter locally after fetch
        user_filter=selected_colab_filter_user_val,
        cliente_id_filter=client_id_filter_val, # Pass ID
        tipos_cliente_filter=tipos_to_pass_manager_val 
    )
    df_docs = pd.DataFrame(docs_to_validate)

    if status_filter_to_pass_val: # Apply multi-status filter locally
          if not df_docs.empty and 'status' in df_docs.columns:
               df_docs = df_docs[df_docs['status'].isin(status_filter_to_pass_val)]
          elif 'status' not in df_docs.columns and not df_docs.empty:
               st.warning("Coluna 'status' não encontrada. Filtro de status não aplicado.")
    
    if not df_docs.empty:
        st.info(f"Exibindo {len(df_docs)} documentos para validação.")
        df_display = df_docs.copy()
        df_display['Marcar para Validar'] = False
        df_display['Novo Status'] = df_display['status'] 
        df_display['Observações'] = df_display['observacoes_validacao'].fillna('')

        cols_to_show_editor = ['Marcar para Validar', 'Novo Status', 'Observações', 'link_ou_documento',
                               'status', 'colaborador_username', 'cliente_nome', 'data_registro', 
                               'dimensao_criterio', 'id', 'data_validacao', 'validado_por']
        cols_to_show_editor = [col for col in cols_to_show_editor if col in df_display.columns]
        column_config = {
            "Marcar para Validar": st.column_config.CheckboxColumn(required=True),
            "Novo Status": st.column_config.SelectboxColumn("Novo Status", options=config.VALID_STATUSES, required=True),
            "Observações": st.column_config.TextColumn("Observações", width="medium"),
            "link_ou_documento": st.column_config.LinkColumn("Link/Documento", width="large", display_text="Abrir/Ver"),
            "status": st.column_config.TextColumn("Status Atual", disabled=True),
            "colaborador_username": st.column_config.TextColumn("Colaborador", disabled=True),
            "cliente_nome": st.column_config.TextColumn("Cliente", disabled=True),
            "data_registro": st.column_config.DateColumn("Data Reg.", format="DD/MM/YYYY", disabled=True),
            "dimensao_criterio": st.column_config.TextColumn("Critério", disabled=True),
            "id": st.column_config.TextColumn("ID", disabled=True, width="small"),
            "data_validacao": st.column_config.DatetimeColumn("Data Validação", format="DD/MM/YYYY HH:mm", disabled=True),
            "validado_por": st.column_config.TextColumn("Validado Por", disabled=True),
        }
        final_column_config = {k: v for k, v in column_config.items() if k in cols_to_show_editor}
        st.markdown("Marque os documentos, selecione o **Novo Status**, adicione observações e clique em 'Processar'.")
        if 'validation_editor_key' not in st.session_state: st.session_state.validation_editor_key = 0
        
        edited_df = st.data_editor(df_display[cols_to_show_editor], column_config=final_column_config, 
                                   key=f"validation_editor_{st.session_state.validation_editor_key}",
                                   hide_index=True, use_container_width=True, num_rows="dynamic", height=600)
        st.divider()
        marked_rows = edited_df[edited_df['Marcar para Validar'] == True]
        num_marked = len(marked_rows)

        if st.button(f"🚀 Processar {num_marked} Validações Marcadas", disabled=(num_marked == 0), type="primary"):
            if num_marked > 0:
                success_count, fail_count = 0, 0
                with st.spinner("Processando validações..."):
                    for index, row in marked_rows.iterrows():
                        update_success = manager.update_document_status_gsheet_and_local(
                            doc_id=row['id'], new_status=row['Novo Status'], 
                            admin_username=admin_username, observacoes=row['Observações']
                        )
                        if update_success: success_count += 1
                        else: fail_count += 1; st.warning(f"Falha ao processar ID: {row['id']}")
                st.toast(f"Processamento concluído!")
                if success_count > 0: st.success(f"{success_count} documentos atualizados!")
                if fail_count > 0: st.error(f"{fail_count} validações falharam.")
                st.session_state.validation_editor_key += 1; st.rerun()
    else: st.info("Nenhum documento encontrado com os filtros selecionados para validação.")