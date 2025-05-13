import streamlit as st
import pandas as pd
from datetime import datetime
import config # Import config

st.set_page_config(layout="wide")

# --- Check Login and Role ---
if not st.session_state.get('logged_in'):
    st.error("Por favor, faça o login para acessar esta página.")
    st.stop()
if st.session_state.get('role') != 'Usuario':
    st.error("Apenas usuários com perfil 'Usuario' podem acessar esta página.")
    st.stop()
if not st.session_state.get('data_loaded') or not st.session_state.get('db_manager'):
    st.warning("Os dados ainda estão sendo carregados ou o gerenciador não foi inicializado.")
    st.stop()


manager = st.session_state.db_manager
username = st.session_state.get('username')
nome_completo = st.session_state.get('nome_completo')

# --- Page Title ---
tab1, tab2, tab3 = st.tabs([
    "Registrar Novo Abastecimento", 
    "Visualizar Abastecimento",
    "🔗 Atribuir Cliente-Colaborador" 
])

with tab1:
    st.markdown("#### ✅ Registrar Novo Abastecimento")
    st.write(f"Registrando como: **{nome_completo}**")
    st.divider()

    # Get Client Options - assigned_clients_local now returns list of dicts {id, nome, tipo}
    assigned_clients_dicts = manager.get_assigned_clients_local(username) 
    
    client_options_display = ["Selecione..."]
    client_name_to_id_map = {}
    if not assigned_clients_dicts:
        st.warning("⚠️ Você não está atribuído a nenhum cliente.")
    else:
        assigned_client_types = sorted(list(set(c['tipo'] for c in assigned_clients_dicts if c['tipo'])))
        
        selected_type_filter_reg = "Todos" # Default
        col1, col2 = st.columns(2)
        with col2:
            if assigned_client_types: # Only show filter if there are types among assigned clients
                filter_options = ["Todos"] + assigned_client_types
                selected_type_filter_reg = st.selectbox(
                    "Filtrar Cliente por Tipo:",
                    options=filter_options,
                    key="reg_tipo_cliente_filter",
                    index=0 # Default to "Todos"
                )

        # --- Filter Clients based on selected type ---
        filtered_clients_for_dropdown = assigned_clients_dicts
        if selected_type_filter_reg != "Todos":
            filtered_clients_for_dropdown = [
                c for c in assigned_clients_dicts if c['tipo'] == selected_type_filter_reg
            ]

        # --- Populate Client Dropdown Options ---
        client_options_display = ["Selecione..."]
        client_name_to_id_map = {}
        if filtered_clients_for_dropdown:
            sorted_clients = sorted(filtered_clients_for_dropdown, key=lambda x: x['nome'])
            for client_dict in sorted_clients:
                client_options_display.append(client_dict['nome'])
                client_name_to_id_map[client_dict['nome']] = client_dict['id']
        else:
            # If filtering resulted in no clients, show a message or disable
             st.caption(f"Nenhum cliente atribuído do tipo '{selected_type_filter_reg}' encontrado.")


    with st.form("abastecimento_form", clear_on_submit=True):
        st.subheader("Detalhes do Registro")
        with col1:
            data_reg = st.date_input("Data do Registro", value=datetime.now().date(), key="form_data_reg")
            cliente_selecionado_nome = st.selectbox( # This is cliente_NOME
                "Cliente Atribuído",
                options=client_options_display,
                key="form_cliente_nome",
                disabled=(not assigned_clients_dicts or not filtered_clients_for_dropdown) 
            )
            dimensao = st.selectbox(
                "Dimensão / Critério",
                options=["Selecione..."] + list(config.CRITERIA_COLORS.keys()),
                key="form_dimensao"
            )
        with col2:
            links_docs_input_for_count = st.session_state.get("form_links", "") # Get current value if available
            num_lines = len([line for line in links_docs_input_for_count.strip().split('\n') if line.strip()]) if links_docs_input_for_count else 0
            quantidade_display = st.number_input("Quantidade (Linhas Inseridas Abaixo)", min_value=0, value=num_lines, step=1, key="form_qtd_display", disabled=True)
            
            status_inicial = st.selectbox(
                "Status Inicial",
                options=[s for s in config.VALID_STATUSES if s not in ['Validado', 'Inválido']],
                index=0,
                key="form_status"
            )

        links_docs = st.text_area(
            "Links Abastecidos ou Nome dos Documentos (UM POR LINHA)",
            height=150,
            key="form_links", # Keep key for session state access
            help="Insira um link ou nome de documento por linha. Cada linha será um registro separado.",
            on_change=None # No specific on_change needed, count happens before display
        )

        submitted = st.form_submit_button("💾 Adicionar Registro(s) Localmente")

        if submitted:
            errors = []
            if not assigned_clients_dicts: errors.append("Nenhum cliente atribuído para registrar.")
            # Check if selection is possible based on filtering
            if cliente_selecionado_nome == "Selecione..." and not filtered_clients_for_dropdown and selected_type_filter_reg != "Todos":
                 errors.append(f"Nenhum cliente do tipo '{selected_type_filter_reg}' disponível para seleção.")
            elif cliente_selecionado_nome == "Selecione...":
                 errors.append("Selecione um cliente.")
            if dimensao == "Selecione...": errors.append("Selecione a dimensão/critério.")

            items = [item.strip() for item in links_docs.strip().split('\n') if item.strip()]
            if not items: errors.append("Insira pelo menos um link ou nome de documento.")

            if errors:
                for error in errors: st.error(error)
            else:
                num_added = 0
                num_failed = 0
                num_duplicates = 0 # Contador para duplicatas
                duplicate_messages = [] # Lista para armazenar mensagens de duplicatas
                
                # Obter cliente_id e cliente_tipo com base no cliente_nome_selecionado e no filtro de tipo
                # Esta lógica assume que client_name_to_id_map e filtered_clients_for_dropdown estão corretos
                
                selected_client_data = next((c for c in filtered_clients_for_dropdown if c['nome'] == cliente_selecionado_nome), None)

                if not selected_client_data:
                    st.error(f"Erro interno: Não foi possível encontrar os dados completos para o cliente '{cliente_selecionado_nome}'.")
                else:
                    cliente_id_selecionado = selected_client_data['id']
                    cliente_tipo_selecionado = selected_client_data['tipo'] # Necessário para a mensagem de erro

                    for item_desc in items:
                        doc_data = {
                            "id": None, # Será gerado em add_documento_local
                            "colaborador_username": username,
                            "cliente_nome": cliente_selecionado_nome, # Mantido para referência, mas cliente_id é a chave
                            "cliente_id": cliente_id_selecionado,
                            "data_registro": data_reg.isoformat() if data_reg else datetime.now().date().isoformat(),
                            "dimensao_criterio": dimensao,
                            "link_ou_documento": item_desc,
                            "quantidade": 1,
                            "status": status_inicial,
                            "data_envio_original": datetime.now().date().isoformat(), # Data de quando foi adicionado localmente
                            "data_validacao": None,
                            "validado_por": None,
                            "observacoes_validacao": None,
                            "is_synced": 0
                        }
                        add_success, message = manager.add_documento_local(doc_data)
                        if add_success:
                            num_added += 1
                        elif message == "DUPLICATE":
                            num_duplicates += 1
                            duplicate_messages.append(
                                f"⚠️ **Duplicado:** O item '{item_desc}' já foi registrado para o cliente '{cliente_selecionado_nome} ({cliente_tipo_selecionado})' na dimensão '{dimensao}'."
                            )
                        else:
                            num_failed += 1
                            st.error(f"Falha ao adicionar '{item_desc}': {message}") # Exibe outras mensagens de erro

                    if num_added > 0: st.success(f"{num_added} registro(s) novo(s) adicionado(s) com sucesso à sua sessão local.")
                    if duplicate_messages:
                        for msg in duplicate_messages:
                            st.warning(msg)
                    if num_failed > 0: st.warning(f"{num_failed} registro(s) falharam ao ser adicionados por outros motivos.")
                    
                    # Limpar o campo de texto após o processamento bem-sucedido ou parcial
                    # if num_added > 0 or num_duplicates > 0 or num_failed > 0: # Rerun se algo aconteceu
                        # st.rerun()  # Rerun para atualizar a contagem e a lista de pendentes
    st.divider()
    st.subheader("Registros Locais Pendentes de Envio")
    unsynced_docs = manager.get_unsynced_documents_local(username)
    if 'editor_key_counter' not in st.session_state: st.session_state.editor_key_counter = 0
    editor_key = f"data_editor_{st.session_state.editor_key_counter}"

    if unsynced_docs:
        df_unsynced = pd.DataFrame([dict(row) for row in unsynced_docs])
        # Ensure 'cliente_nome' is present for display if 'cliente_id' is the primary key
        if 'cliente_id' in df_unsynced.columns and 'cliente_nome' not in df_unsynced.columns:
             # Fetch client names if only IDs are present (shouldn't be the case with current add_documento_local)
             all_clients_df = pd.DataFrame(manager.listar_clientes_local())
             if not all_clients_df.empty:
                  df_unsynced = pd.merge(df_unsynced, all_clients_df[['id', 'nome']], left_on='cliente_id', right_on='id', how='left', suffixes=('', '_cliente'))
                  df_unsynced.rename(columns={'nome_cliente': 'cliente_nome'}, inplace=True)
                  df_unsynced.drop(columns=['id_cliente'], errors='ignore', inplace=True)


        cols_to_show = ['data_registro', 'cliente_nome', 'dimensao_criterio', 'link_ou_documento', 'status', 'id']
        df_display = df_unsynced[[col for col in cols_to_show if col in df_unsynced.columns]].copy()
        df_display.insert(0, "Selecionar", False)

        column_config_unsync = {
            "Selecionar": st.column_config.CheckboxColumn("Selecionar", required=True),
            "id": st.column_config.TextColumn("ID", disabled=True, help="ID único do documento"),
            "data_registro": st.column_config.DateColumn("Data Reg.", format="DD/MM/YYYY", disabled=True),
            "cliente_nome": st.column_config.TextColumn("Cliente", disabled=True),
            "dimensao_criterio": st.column_config.TextColumn("Critério", disabled=True),
            "link_ou_documento": st.column_config.TextColumn("Link/Doc", width="large", disabled=True),
            "status": st.column_config.TextColumn("Status", disabled=True),
        }
        final_column_config_unsync = {k:v for k,v in column_config_unsync.items() if k in df_display.columns}

        st.info("Marque os registros que deseja enviar para a planilha e clique em 'Salvar Selecionados'.")
        edited_df_unsync = st.data_editor(df_display, column_config=final_column_config_unsync, key=editor_key,
                                          hide_index=True, use_container_width=True, num_rows="dynamic")
        selected_rows_unsync = edited_df_unsync[edited_df_unsync["Selecionar"] == True]
        selected_ids_unsync = selected_rows_unsync["id"].tolist() if not selected_rows_unsync.empty else []
        st.markdown(f"**{len(selected_ids_unsync)}** registro(s) selecionado(s).")
        st.divider()
        if st.button("📤 Salvar Selecionados na Planilha", disabled=(not selected_ids_unsync), type="primary"):
            if selected_ids_unsync:
                with st.spinner("Enviando dados selecionados para a planilha..."):
                    save_success = manager.save_selected_docs_to_sheets(username, selected_ids_unsync)
                if save_success:
                    st.success(f"{len(selected_ids_unsync)} registros selecionados foram enviados com sucesso!")
                    st.session_state.editor_key_counter += 1; st.rerun()
                else: st.error("Falha ao salvar os registros selecionados na planilha.")
            else: st.warning("Nenhum registro foi selecionado para salvar.")
    else: st.info("Nenhum registro local pendente de envio.")
    final_check_unsaved = manager.get_unsynced_documents_local(username)
    st.session_state['unsaved_changes'] = bool(final_check_unsaved)
    
    
with tab2:
    st.markdown(f"#### 📋 Meus Registros - {nome_completo}")
    st.write("Acompanhe aqui o status dos seus envios.")
    st.divider()

    st.header("Filtros Meus Registros")
    col1, col2, col3 = st.columns(3)
    # Filter by Client Type
    all_client_info_for_user = manager.get_assigned_clients_local(username) # Gets list of dicts
    available_client_types_user = sorted(list(set(c['tipo'] for c in all_client_info_for_user if c['tipo'])))
    
    selected_tipos_filter_user = ["Todos"]
    if available_client_types_user:
        with col1:
            selected_tipos_filter_user = st.multiselect(
                "Filtrar por Tipo de Cliente:",
                options=available_client_types_user,
                key="my_records_tipo_cliente_filter"
            )
        if not selected_tipos_filter_user: # If user deselects all, treat as "Todos"
            selected_tipos_filter_user = ["Todos"]

    # Filter by Client (clients assigned to this user, potentially filtered by type)
    # Filter assigned_clients list based on selected_tipos_filter_user
    clients_for_user_display = ["Todos"]
    clients_for_user_map = {"Todos": None} # name: id
    
    if all_client_info_for_user:
        filtered_clients_by_type = all_client_info_for_user
        if "Todos" not in selected_tipos_filter_user and selected_tipos_filter_user:
            filtered_clients_by_type = [
                c for c in all_client_info_for_user if c['tipo'] in selected_tipos_filter_user
            ]
        
        sorted_filtered_clients = sorted(filtered_clients_by_type, key=lambda x: x['nome'])
        for client_dict in sorted_filtered_clients:
            clients_for_user_display.append(client_dict['nome'])
            clients_for_user_map[client_dict['nome']] = client_dict['id']
    with col2:
        selected_client_name_my_records = st.selectbox(
            "Filtrar por Cliente:",
            options=clients_for_user_display,
            key="my_records_client_filter"
        )
    selected_client_id_my_records = clients_for_user_map.get(selected_client_name_my_records)


    status_options = ["Todos"] + config.VALID_STATUSES
    with col3:
        selected_status_filter = st.selectbox(
            "Filtrar por Status:",
            options=status_options,
            key="my_records_status_filter"
        )

    
    
    # Fetch documents for the user, applying type filter at DB level
    user_documents_raw = manager.get_documentos_usuario_local(
        username=username, 
        synced_status=None, # Get all (synced and unsynced)
        tipos_cliente_filter=selected_tipos_filter_user if "Todos" not in selected_tipos_filter_user else None
    )

    if not user_documents_raw:
        st.info("Você ainda não possui nenhum registro ou nenhum registro corresponde aos filtros.")
        st.stop()

    df_user_docs = pd.DataFrame([dict(row) for row in user_documents_raw])

    df_filtered = df_user_docs.copy()
    if selected_client_id_my_records: # Filter by specific client ID if selected
        df_filtered = df_filtered[df_filtered['cliente_id'] == selected_client_id_my_records]
    
    if selected_status_filter != "Todos":
        df_filtered = df_filtered[df_filtered['status'] == selected_status_filter]

    if df_filtered.empty:
        st.info("Nenhum registro encontrado com os filtros selecionados.")
    else:
        st.info(f"Exibindo {len(df_filtered)} de {len(df_user_docs)} registros (considerando filtro de tipo de cliente na busca inicial).")

    st.divider()
    st.subheader("Resumo dos Seus Registros (com filtros aplicados):")
    status_counts = df_filtered['status'].value_counts().reindex(config.VALID_STATUSES, fill_value=0)
    cols_stats = st.columns(len(config.VALID_STATUSES))
    
    for i, status_name in enumerate(config.VALID_STATUSES):
        count = status_counts.get(status_name, 0)
        cols_stats[i].metric(label=status_name, value=count)
    
        def format_display_date(date_str, fmt="%d/%m/%Y"):
            if not date_str or pd.isna(date_str) or str(date_str).lower() == 'none': return "N/A"
            try: return pd.to_datetime(date_str).strftime(fmt)
            except: return str(date_str)
        def format_display_datetime(dt_str, fmt="%d/%m/%Y %H:%M"):
            if not dt_str or pd.isna(dt_str) or str(dt_str).lower() == 'none': return "N/A"
            try: return pd.to_datetime(dt_str).strftime(fmt)
            except: return str(dt_str)

        df_display = df_filtered.copy()
        if 'data_registro' in df_display.columns: df_display['Data Registro'] = df_display['data_registro'].apply(format_display_date)
        if 'data_validacao' in df_display.columns: df_display['Data Validação'] = df_display['data_validacao'].apply(format_display_datetime)
        
        column_rename_map = {
            'cliente_nome': 'Cliente', 'dimensao_criterio': 'Critério', 'link_ou_documento': 'Link/Documento',
            'quantidade': 'Qtd.', 'status': 'Status', 'validado_por': 'Validado Por',
            'observacoes_validacao': 'Observações', 'is_synced': 'Sincronizado'
        }
        df_display.rename(columns={k: v for k, v in column_rename_map.items() if k in df_display.columns}, inplace=True)
        
        # Ensure 'Sincronizado' column indicates pending if is_synced is 0
        if 'Sincronizado' in df_display.columns:
             df_display['Sincronizado'] = df_display['Sincronizado'].apply(lambda x: 'Pendente' if str(x) == '0' else ('Sim' if str(x) == '1' else 'N/A'))


        display_columns_ordered = ['Data Registro', 'Cliente', 'Critério', 'Link/Documento', 'Qtd.', 
                                   'Status', 'Data Validação', 'Validado Por', 'Observações', 'Sincronizado', 'id']
        final_display_cols = [col for col in display_columns_ordered if col in df_display.columns]
        
        column_config_display = {"Link/Documento": st.column_config.LinkColumn("Link/Documento", display_text="Abrir/Ver"), 
                                 "id": st.column_config.TextColumn("ID (Ref.)", width="small")}
        for col_name in final_display_cols:
            if col_name not in column_config_display: 
                column_config_display[col_name] = st.column_config.TextColumn(col_name, disabled=True)
        
    st.dataframe(df_display[final_display_cols], column_config=column_config_display, hide_index=True, use_container_width=True)


with tab3:
    st.subheader(f"Meus Clientes Atribuídos")
    st.caption(f"Visualizando clientes atribuídos a: **{nome_completo}** ({username})")
    st.divider()

    # Obter clientes atribuídos ao usuário logado (username)
    assigned_clients_to_user = manager.get_assigned_clients_local(username)

    if not assigned_clients_to_user:
        st.info("Você não está atualmente atribuído a nenhum cliente.")
    else:
        client_types_of_user = sorted(list(set(c['tipo'] for c in assigned_clients_to_user if c['tipo'])))
        selected_type_filter_my_clients = "Todos"
        if client_types_of_user:
            filter_options_my_clients = ["Todos"] + client_types_of_user
            selected_type_filter_my_clients = st.selectbox(
                "Filtrar seus clientes atribuídos por Tipo:",
                options=filter_options_my_clients,
                key="my_assigned_clients_type_filter_abastecimento"
            )

        clients_to_display_list = assigned_clients_to_user
        if selected_type_filter_my_clients != "Todos":
            clients_to_display_list = [
                c for c in assigned_clients_to_user if c['tipo'] == selected_type_filter_my_clients
            ]

        if not clients_to_display_list:
            st.info(f"Nenhum cliente atribuído a você corresponde ao tipo '{selected_type_filter_my_clients}'.")
        else:
            st.markdown(f"**Total de clientes atribuídos (correspondendo ao filtro): {len(clients_to_display_list)}**")
            df_display_my_clients = pd.DataFrame(clients_to_display_list)
            if not df_display_my_clients.empty:
                df_display_my_clients_view = df_display_my_clients[['nome', 'tipo']].copy().sort_values(by=['nome'])
                df_display_my_clients_view.rename(columns={'nome': 'Nome do Cliente', 'tipo': 'Tipo do Cliente'}, inplace=True)
                st.dataframe(
                    df_display_my_clients_view,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "Nome do Cliente": st.column_config.TextColumn("Nome do Cliente"),
                        "Tipo do Cliente": st.column_config.TextColumn("Tipo do Cliente"),
                    }
                )

    st.divider()
    st.subheader("Atribuir Novos Clientes a Mim")

    all_system_clients = manager.listar_clientes_local() # Lista de {'id', 'nome', 'tipo'}
    assigned_client_ids_for_user = [c['id'] for c in assigned_clients_to_user]

    available_for_self_assignment = [
        client for client in all_system_clients
        if client['id'] not in assigned_client_ids_for_user
    ]

    if not available_for_self_assignment:
        st.info("Não há novos clientes disponíveis no sistema para autoatribuição ou todos os clientes já estão atribuídos a você.")
    else:
        available_client_types_for_self_assign = sorted(list(set(c['tipo'] for c in available_for_self_assignment if c['tipo'])))
        selected_available_type_filter_self_assign = "Todos"
        if available_client_types_for_self_assign:
            filter_options_available_self_assign = ["Todos"] + available_client_types_for_self_assign
            selected_available_type_filter_self_assign = st.selectbox(
                "Filtrar clientes disponíveis para atribuição por Tipo:",
                options=filter_options_available_self_assign,
                key="self_assign_available_type_filter"
            )

        clients_to_offer_for_assignment = available_for_self_assignment
        if selected_available_type_filter_self_assign != "Todos":
            clients_to_offer_for_assignment = [
                c for c in available_for_self_assignment if c['tipo'] == selected_available_type_filter_self_assign
            ]

        if not clients_to_offer_for_assignment:
            st.info(f"Nenhum cliente disponível do tipo '{selected_available_type_filter_self_assign}' para autoatribuição.")
        else:
            client_id_to_display_map_self_assign = {
                client['id']: f"{client['nome']} ({client['tipo']})"
                for client in clients_to_offer_for_assignment
            }
            multiselect_options_ids_self_assign = list(client_id_to_display_map_self_assign.keys())

            def format_client_for_self_assign_multiselect(client_id):
                return client_id_to_display_map_self_assign.get(client_id, client_id)

            selected_ids_to_self_assign = st.multiselect(
                "Selecione clientes para atribuir a você:",
                options=multiselect_options_ids_self_assign,
                format_func=format_client_for_self_assign_multiselect,
                key="self_assign_clients_multiselect"
            )

            if st.button("➕ Atribuir Selecionados a Mim"):
                if selected_ids_to_self_assign:
                    with st.spinner("Atribuindo clientes..."):
                        assign_success = manager.assign_clients_to_collab(username, selected_ids_to_self_assign)
                        if assign_success:
                            st.success("Clientes atribuídos com sucesso! A lista será atualizada.")
                            st.rerun()
                        else:
                            st.error("Falha ao atribuir os clientes selecionados.")
                else:
                    st.warning("Nenhum cliente selecionado para atribuir.")