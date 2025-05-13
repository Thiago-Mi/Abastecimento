import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import config # Import config
from datetime import datetime

st.set_page_config(layout="wide")

# --- Check Login and Data Load Status ---
if not st.session_state.get('logged_in'):
    st.error("Por favor, faça o login para acessar esta página.")
    st.stop()
if not st.session_state.get('data_loaded') or not st.session_state.get('db_manager'):
    st.warning("Os dados ainda estão sendo carregados ou o gerenciador não foi inicializado.")
    st.stop()

manager = st.session_state.db_manager
role = st.session_state.get('role')
username = st.session_state.get('username')
nome_completo = st.session_state.get('nome_completo')

# For Cliente role
cliente_id_logado = st.session_state.get('cliente_id_logado') # Get ID for client role
cliente_nome_logado = st.session_state.get('cliente_nome')


# --- Page Title ---
st.markdown("#### 🗓️ Acompanhamento Abastecimento - Atricon 2025")
st.divider()

# ======================================================
# RENDERIZAÇÃO CONDICIONAL BASEADA NA ROLE
# ======================================================

# ------------------- VISTA DO CLIENTE -------------------
if role == 'Cliente':
    if not cliente_id_logado: # Check ID now
         st.error("ID do cliente associado não encontrado.")
         st.stop()

    selected_period_label = st.session_state.get('selected_period', "Todos")
    periodo_dias_map = {"Últimos 7 dias": 7, "Últimos 30 dias": 30, "Últimos 90 dias": 90}
    periodo_dias_filter = periodo_dias_map.get(selected_period_label) 

    kpi_cliente = manager.get_kpi_data_local(
        cliente_id=cliente_id_logado, # Use cliente_id
        periodo_dias=periodo_dias_filter
    )
    kp1, kp2, kp3 = st.columns(3)
    kp1.metric("Docs Pendentes", f"{kpi_cliente.get('docs_enviados', 0):02d}")
    kp2.metric("Docs Inválidos", f"{kpi_cliente.get('docs_invalidos', 0):02d}") # Assuming 'Pendentes' maps to 'invalidos' KPI key for now
    kp3.metric("Docs Validados", f"{kpi_cliente.get('docs_validados', 0):02d}")
    st.markdown("---") 

    st.subheader("Desempenho Temporal")
    grupo_tempo = 'W' 
    # Pass cliente_id to get_docs_por_periodo_cliente_local
    df_line_cliente = manager.get_docs_por_periodo_cliente_local(cliente_id=cliente_id_logado, grupo=grupo_tempo)


    if not df_line_cliente.empty and 'periodo_dt' in df_line_cliente.columns and 'contagem' in df_line_cliente.columns and df_line_cliente['contagem'].sum() > 0:
         fig_scatter_cli = px.scatter(df_line_cliente,
                                      x='periodo_dt', y='contagem', size='contagem', text='contagem',
                                      labels={'periodo_dt': 'Data', 'contagem': 'Docs Validados'}, size_max=15)
         fig_scatter_cli.update_traces(textposition='top center', marker=dict(line=dict(width=1, color='DarkSlateGrey')))
         fig_scatter_cli.update_layout(yaxis_title="Quantidade Validada", xaxis_title="Período (Início da Semana)",
                                       height=350, margin=dict(l=20, r=20, t=30, b=20),
                                       xaxis_tickformat='%d/%m/%Y', showlegend=False)
         st.plotly_chart(fig_scatter_cli, use_container_width=True)
    else:
         st.caption("Nenhum dado para exibir o gráfico de desempenho temporal.")
    st.markdown("---")

    st.subheader("📊 Status Geral")
    # Use cliente_id_logado for analysis
    analysis_data = manager.get_analise_cliente_data_local(cliente_id=cliente_id_logado)


    col_an1, col_an2 = st.columns(2)
    with col_an1: 
        docs_drive = analysis_data['total_documentos_cliente'] # This is total for the client
        docs_pub = analysis_data['docs_validados']
        docs_pend = analysis_data['docs_invalidos'] # This is total_docs - validated_docs
        
        # Displaying sum of validated and pending as "Documentos no Drive" implies these are the only two states considered for this KPI
        st.markdown(f"🟢 Documentos no Registrados - **{docs_pub + docs_pend}**")
        st.markdown(f"🔵 Documentos Validados - **{docs_pub}**")
        st.markdown(f"🔴 Documentos Pendentes/Inválidos - **{docs_pend}**")

        labels_status = ['Validados', 'Pendentes/Inválidos']
        values_status = [docs_pub, docs_pend]
        colors_status = ['#1f77b4', '#d62728']

        if sum(values_status) > 0 : # Only show chart if there are any docs
            fig_donut_status = go.Figure(data=[go.Pie(labels=labels_status, values=values_status, hole=.4,
                                                    marker_colors=colors_status, pull=[0.02, 0.02], sort=False)])
            fig_donut_status.update_layout(showlegend=False, height=300, margin=dict(t=15, b=10, l=10, r=10))
            st.plotly_chart(fig_donut_status, use_container_width=True)
        else:
            st.caption("Nenhum documento para análise de status.")

    with col_an2: 
        st.markdown("**Documentos Validados por Critério**") # Clarified: Shows validated docs per criteria
        crit_counts = analysis_data.get('criterios_counts', {}) # This from get_analise_cliente_data_local should be validated counts per criteria

        labels_crit, values_crit, colors_crit = [], [], []
        for crit_name, color in config.CRITERIA_COLORS.items():
             count = crit_counts.get(crit_name, 0)
             st.markdown(f'<span style="color:{color}; font-size: 1.1em;">■</span> {crit_name} - **{count}**', unsafe_allow_html=True)
             if count > 0: 
                 labels_crit.append(crit_name)
                 values_crit.append(count)
                 colors_crit.append(color)
        if sum(values_crit) > 0:
            fig_donut_crit = go.Figure(data=[go.Pie(labels=labels_crit, values=values_crit, hole=.4,
                                                     marker_colors=colors_crit, pull=[0.02] * len(labels_crit))])
            fig_donut_crit.update_layout(showlegend=False, height=300, margin=dict(t=15, b=10, l=10, r=10))
            st.plotly_chart(fig_donut_crit, use_container_width=True)
        else:
             st.caption("Nenhum documento validado classificado por critério.")


# ------------------- VISTA ADMIN / USUARIO -------------------
elif role in ['Admin', 'Usuario']:

    st.header("Filtros Dashboard")
    col1, col2, col3 = st.columns(3)
    selected_colab_filter_user = None
    if role == 'Admin':
        colaboradores = manager.listar_colaboradores_local()
        colab_options_map = {"Todos": None}
        colab_options_map.update({c['nome_completo']: c['username'] for c in colaboradores})
        with col1:
            selected_colab_name = st.selectbox("Selecione Colaborador:", list(colab_options_map.keys()), index=0)
        selected_colab_filter_user = colab_options_map[selected_colab_name]
    else: 
        with col1:
            st.write(f"**Colaborador:** {nome_completo}")
        selected_colab_filter_user = username

    # --- Filtro Tipo de Cliente ---
    all_client_types_dicts = manager.listar_clientes_local() # Get all clients to extract types
    available_client_types = sorted(list(set(c['tipo'] for c in all_client_types_dicts if c['tipo'])))
    
    selected_tipos_clientes_filter = ["Todos"]
    if available_client_types: # Only show if there are types
        with col2:
            selected_tipos_clientes_filter = st.multiselect(
                "Filtrar por Tipo de Cliente:",
                options=available_client_types, # No "Todos" needed for multiselect default
                key="admin_tipo_cliente_filter"
            )
        if not selected_tipos_clientes_filter: # If user deselects all, treat as "Todos"
            selected_tipos_clientes_filter = ["Todos"]


    # Get clients relevant to the selection (colaborador and type)
    clientes_list_dicts = manager.listar_clientes_local(
        colaborador_username=selected_colab_filter_user,
        tipos_filter=selected_tipos_clientes_filter if "Todos" not in selected_tipos_clientes_filter else None
    )
    
    client_options_map = {"Todos": None} # Stores name: id
    if clientes_list_dicts:
        client_options_map.update({c['nome']: c['id'] for c in clientes_list_dicts})
    with col3:
        selected_client_name_filter = st.selectbox(
            "Selecione Cliente:",
            list(client_options_map.keys()), # Uses names for display
            key="admin_client_name_filter",
            disabled=(selected_colab_filter_user is not None and not clientes_list_dicts and "Todos" not in selected_tipos_clientes_filter)
        )
        selected_client_id_filter = client_options_map.get(selected_client_name_filter) # Get ID for filtering

        if selected_client_name_filter == "Todos" and selected_colab_filter_user and "Todos" in selected_tipos_clientes_filter :
            st.caption("Exibindo dados agregados para o colaborador.")
        elif selected_client_name_filter == "Todos" and "Todos" not in selected_tipos_clientes_filter:
            st.caption(f"Exibindo dados agregados para Tipos: {', '.join(selected_tipos_clientes_filter)}.")


    # --- KPIs Admin/Usuario ---
    # KPI data needs to be aware of the client_id_filter and tipos_cliente_filter
    kpi_geral = manager.get_kpi_data_local(
        colaborador_username=selected_colab_filter_user,
        cliente_id=selected_client_id_filter, # Pass ID
        tipos_cliente_filter=selected_tipos_clientes_filter if "Todos" not in selected_tipos_clientes_filter else None
    )
    kp1, kp2, kp3 = st.columns(3) # Removed one KPI to match client view for now
    kp1.metric("Links Pendentes", f"{kpi_geral.get('docs_enviados', 0):02d}") 
    kp2.metric("Links Validados", f"{kpi_geral.get('docs_validados', 0):02d}") 
    kp3.metric("Links Inválidos", f"{kpi_geral.get('docs_invalidos', 0):02d}")

    st.subheader("🏆 Ranking de Colaboradores por Pontuação")
    df_pontuacao = manager.calcular_pontuacao_colaboradores_gsheet() # Uses local cache; GSheet version is in Admin panel

    if not df_pontuacao.empty:
        df_display = df_pontuacao.head(15).sort_values(by='Pontuação', ascending=True) # Ascending for horizontal bar
        labels = [f"{row['Links Validados']} ({row['Percentual']:.1f}%)" for idx, row in df_display.iterrows()]
        colors = [config.DEFAULT_BAR_COLOR] * len(df_display)
        if selected_colab_filter_user:
             selected_user_details = manager.buscar_usuario_local(selected_colab_filter_user)
             if selected_user_details and selected_user_details['nome_completo'] in df_display.index:
                  try:
                       idx_pos = df_display.index.get_loc(selected_user_details['nome_completo'])
                       colors[idx_pos] = config.HIGHLIGHT_BAR_COLOR
                  except KeyError: pass
        fig_bar_rank = go.Figure(go.Bar(
            y=df_display.index, x=df_display['Pontuação'], text=labels, orientation='h',
            textposition='auto', marker_color=colors))
        fig_bar_rank.update_layout(yaxis_title="Colaborador", xaxis_title="Pontuação", height=400,
                                   margin=dict(l=150, r=10, t=10, b=40), yaxis={'categoryorder':'total ascending'}) # Ensure y-axis matches sorted data
        st.plotly_chart(fig_bar_rank, use_container_width=True)
    else:
        st.info("Ainda não há dados de pontuação para exibir (ranking local).")
    st.divider()

    st.subheader("📊 Análise por Cliente")
    
    # client_for_analysis is the NAME, we need the ID for the manager method
    client_id_for_analysis = selected_client_id_filter 

    if client_id_for_analysis: # Check if a specific client ID is selected
        st.info(f"**Cliente Selecionado:** {selected_client_name_filter}") # Display name

        collab_filter_for_analysis = username if role == 'Usuario' else selected_colab_filter_user # Admin can see specific collab's view of client
        
        # Pass client_id_for_analysis
        analysis_data = manager.get_analise_cliente_data_local(
            cliente_id=client_id_for_analysis,
            colaborador_username=collab_filter_for_analysis
        )

        col_an1, col_an2 = st.columns(2)
        with col_an1: 
            st.markdown("**Status Geral do Cliente**")
            docs_total_client = analysis_data['total_documentos_cliente']
            docs_pub = analysis_data['docs_validados']
            docs_pend = analysis_data['docs_invalidos']
            st.markdown(f"🟢 Documentos Registrado - **{docs_total_client}**")
            st.markdown(f"🔵 Documentos Validados - **{docs_pub}**")
            st.markdown(f"🔴 Documentos Pendentes/Inválidos - **{docs_pend}**")

            labels_status = ['Validados', 'Pendentes/Inválidos']
            values_status = [docs_pub, docs_pend]
            colors_status = ['#1f77b4', '#d62728'] 

            if sum(values_status) > 0 : 
                fig_donut_status = go.Figure(data=[go.Pie(labels=labels_status, values=values_status, hole=.4,
                                                        marker_colors=colors_status, pull=[0.02, 0.02], sort=False)])
                fig_donut_status.update_layout(showlegend=False, height=300, margin=dict(t=15, b=10, l=10, r=10))
                st.plotly_chart(fig_donut_status, use_container_width=True)
            else:
                st.caption("Nenhum documento para análise de status deste cliente.")
        with col_an2: 
            st.markdown("**Documentos Validados por Critério**")
            crit_counts = analysis_data.get('criterios_counts', {}) # validated counts per criteria

            labels_crit, values_crit, colors_crit = [], [], []
            for crit_name, color in config.CRITERIA_COLORS.items():
                 count = crit_counts.get(crit_name, 0)
                 st.markdown(f'<span style="color:{color}; font-size: 1.1em;">■</span> {crit_name} - **{count}**', unsafe_allow_html=True)
                 if count > 0: 
                     labels_crit.append(crit_name)
                     values_crit.append(count)
                     colors_crit.append(color)
            if sum(values_crit) > 0:
                fig_donut_crit = go.Figure(data=[go.Pie(labels=labels_crit, values=values_crit, hole=.4,
                                                         marker_colors=colors_crit, pull=[0.02] * len(labels_crit))])
                fig_donut_crit.update_layout(showlegend=False, height=300, margin=dict(t=15, b=10, l=10, r=10))
                st.plotly_chart(fig_donut_crit, use_container_width=True)
            else:
                 st.caption("Nenhum documento validado classificado por critério para este cliente.")
    elif "Todos" not in selected_tipos_clientes_filter and selected_tipos_clientes_filter : # If types are selected but not a specific client
        st.info(f"Exibindo KPIs agregados para os tipos de cliente selecionados: {', '.join(selected_tipos_clientes_filter)}. Selecione um cliente específico para análise detalhada.")
    else: # No specific client or type selected for detailed analysis
        st.info("⬅️ Selecione um cliente e/ou tipo de cliente na barra lateral para ver a análise detalhada.")
else:
    st.error("Perfil de usuário desconhecido.")