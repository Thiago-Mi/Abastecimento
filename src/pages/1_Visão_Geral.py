# pages/1_Visão_Geral.py
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
cliente_nome_logado = st.session_state.get('cliente_nome') # Used only if role is Cliente

# --- Page Title ---
st.markdown("#### 🗓️ Acompanhamento Abastecimento - Atricon 2025")
st.divider()

# ======================================================
# RENDERIZAÇÃO CONDICIONAL BASEADA NA ROLE
# ======================================================

# ------------------- VISTA DO CLIENTE -------------------
if role == 'Cliente':
    if not cliente_nome_logado:
         st.error("Nome do cliente associado não encontrado.")
         st.stop()

    st.info(f"Exibindo dados para o cliente: **{cliente_nome_logado}**")

    # --- Sidebar Filter (already in streamlit_app.py) ---
    selected_period_label = st.session_state.get('selected_period', "Todos")
    periodo_dias_map = {"Últimos 7 dias": 7, "Últimos 30 dias": 30, "Últimos 90 dias": 90}
    periodo_dias_filter = periodo_dias_map.get(selected_period_label) # None if "Todos"

    # --- KPIs Cliente ---
    kpi_cliente = manager.get_kpi_data_local(
        cliente_nome=cliente_nome_logado,
        periodo_dias=periodo_dias_filter
    )
    kp1, kp2, kp3 = st.columns(3)
    kp1.metric("Docs Enviados", f"{kpi_cliente.get('docs_enviados', 0):02d}")
    kp2.metric("Docs Publicados", f"{kpi_cliente.get('docs_publicados', 0):02d}")
    kp3.metric("Docs Pendentes", f"{kpi_cliente.get('docs_pendentes', 0):02d}")
    st.markdown("---") # Visual separator like the image

    # --- Gráfico de Linha Cliente ---
    st.subheader("Desempenho Temporal (Docs Publicados)")
    grupo_tempo = 'W' # Default Semanal, pode ser dinâmico se desejar
    df_line_cliente = manager.get_docs_por_periodo_cliente_local(cliente_nome_logado, grupo=grupo_tempo)

    if not df_line_cliente.empty and 'periodo_dt' in df_line_cliente.columns:
         fig_line_cli = px.line(df_line_cliente, x='periodo_dt', y='contagem', markers=True,
                             labels={'periodo_dt': 'Período', 'contagem': 'Docs Publicados'})
         fig_line_cli.update_layout(
             yaxis_title="Quantidade Publicada",xaxis_title="",
             height=300, margin=dict(l=10, r=10, t=10, b=10)
         )
         # Add peak annotation if desired
         if not df_line_cliente.empty:
             try: # Handle potential errors if no data
                  peak_idx = df_line_cliente['contagem'].idxmax()
                  peak_row = df_line_cliente.loc[peak_idx]
                  fig_line_cli.add_annotation(x=peak_row['periodo_dt'], y=peak_row['contagem'],
                                           text=f"<b>{peak_row['contagem']}</b>", showarrow=True, arrowhead=1,
                                           bordercolor="#636EFA", borderwidth=1, bgcolor="#636EFA", font=dict(color="white"),
                                           yshift=10 # Adjust position slightly
                                          )
             except Exception as peak_err: print(f"Could not add peak annotation: {peak_err}")


         st.plotly_chart(fig_line_cli, use_container_width=True)
    else:
         st.caption("Nenhum dado para exibir o gráfico de desempenho temporal.")

    st.markdown("---") # Visual separator

    # --- Critérios Atendidos Cliente ---
    st.subheader("Critérios Atendidos")
    crit_data_cliente = manager.get_criterios_atendidos_cliente_local(cliente_nome_logado)

    if not crit_data_cliente or all(v['total'] == 0 for v in crit_data_cliente.values()):
         st.info("Nenhum dado de critério encontrado para este cliente.")
    else:
         max_total_crit = 1 # Avoid division by zero if no criteria have docs
         totals = [data['total'] for data in crit_data_cliente.values() if data['total'] > 0]
         if totals: max_total_crit = max(totals)

         for criterio, data in crit_data_cliente.items():
              total = data.get('total', 0)
              atendidos = data.get('atendidos', 0)
              # Percent relative to ITSELF, not overall total? Image implies percentage of total docs for that criterion.
              percentual = (atendidos / total * 100) if total > 0 else 0
              # Or percent relative to max total for scaling? Let's use percent of its own total.

              color = config.CRITERIA_COLORS.get(criterio, config.DEFAULT_CRITERIA_COLOR)

              col_cor, col_nome, col_barra_texto = st.columns([0.05, 0.2, 0.75])

              with col_cor:
                   st.markdown(f'<div style="width: 20px; height: 20px; background-color: {color}; margin-top: 5px;"></div>', unsafe_allow_html=True)
              with col_nome:
                   st.write(f"**{criterio}**")
              with col_barra_texto:
                    st.progress(percentual / 100)
                    st.caption(f"{atendidos} docs / {percentual:.0f}%")


# ------------------- VISTA ADMIN / USUARIO -------------------
elif role in ['Admin', 'Usuario']:

    # --- Sidebar Filters ---
    st.sidebar.header("Filtros Dashboard")

    # Colaborador Filter (Admin only)
    selected_colab_filter_user = None # Username for filtering data
    if role == 'Admin':
        colaboradores = manager.listar_colaboradores_local()
        colab_options_map = {"Todos": None}
        colab_options_map.update({c['nome_completo']: c['username'] for c in colaboradores})
        selected_colab_name = st.sidebar.selectbox("Selecione Colaborador:", list(colab_options_map.keys()))
        selected_colab_filter_user = colab_options_map[selected_colab_name]
    else: # Usuario sees their own data primarily
         st.sidebar.write(f"**Colaborador:** {nome_completo}")
         selected_colab_filter_user = username

    # Get clients relevant to the selection
    clientes_list = manager.listar_clientes_local(colaborador_username=selected_colab_filter_user)
    client_options = {"Todos": None}
    client_options.update({c['nome']: c['id'] for c in clientes_list} if clientes_list else {}) # Using ID might be needed if name filter doesn't work
    client_options_names_only = ["Todos"] + sorted([c['nome'] for c in clientes_list]) if clientes_list else ["Todos"]


    selected_client_name_filter = st.sidebar.selectbox(
        "Selecione Cliente:",
        client_options_names_only,
        key="admin_client_filter",
        # Disable if showing 'Todos' collaborators and no clients? Or just show all clients?
        # Let's allow selecting any client if 'Todos' collaborators selected
        disabled= (selected_colab_filter_user is not None and not clientes_list) # Disable if specific user has no clients
    )
    if selected_client_name_filter == "Todos" and selected_colab_filter_user:
        # If user selected 'Todos' clients, but a specific collab, show all clients for that collab
        st.sidebar.caption("Exibindo todos os clientes atribuídos.")


    # --- KPIs Admin/Usuario ---
    kpi_geral = manager.get_kpi_data_local(colaborador_username=selected_colab_filter_user) # Filter by selected collab
    kp1, kp2, kp3, kp4 = st.columns(4)
    # Rename based on Layout 2 image
    kp1.metric("Links Enviados", f"{kpi_geral.get('docs_enviados', 0):02d}") # Assumes docs_enviados maps here
    kp2.metric("Links Validados", f"{kpi_geral.get('docs_publicados', 0):02d}") # Assumes docs_publicados maps here
    kp3.metric("Links Pendentes", f"{kpi_geral.get('docs_pendentes', 0):02d}")
    kp4.metric("Links Inválidos", f"{kpi_geral.get('docs_invalidos', 0):02d}")
    st.divider()

    # --- Gráfico de Barras Ranking (Show always?) ---
    st.subheader("🏆 Ranking de Colaboradores por Pontuação")
    df_pontuacao = manager.calcular_pontuacao_colaboradores_gsheet()

    if not df_pontuacao.empty:
        # Limit number displayed? Layout shows ~6-7. Use Top 15 for scrollability?
        df_display = df_pontuacao.head(15).sort_values(by='Pontuação', ascending=False) # Descending for vertical bar

        # Percentage is calculated globally in the manager method now.
        # Calculate percentage relative to THIS displayed subset? No, global % is better.
        labels = [f"{row['Links Validados']} ({row['Percentual']:.1f}%)" for idx, row in df_display.iterrows()]
        colors = [config.DEFAULT_BAR_COLOR] * len(df_display)
        # Highlight selected collaborator?
        if selected_colab_filter_user:
             selected_user_details = manager.buscar_usuario_local(selected_colab_filter_user)
             if selected_user_details and selected_user_details['nome_completo'] in df_display.index:
                  try:
                       idx_pos = df_display.index.get_loc(selected_user_details['nome_completo'])
                       colors[idx_pos] = config.HIGHLIGHT_BAR_COLOR
                  except KeyError: pass

        # Create Vertical Bar Chart
        fig_bar_rank = go.Figure(go.Bar(
            x=df_display.index,       # Colaborador Names
            y=df_display['Pontuação'], # Score
            text=labels,              # Use calculated labels
            textposition='auto',      # Let Plotly decide best position (inside/outside)
            marker_color=colors       # Use defined colors
        ))
        fig_bar_rank.update_layout(
            #title="Pontuação por Colaborador", # Subheader serves as title
            xaxis_title="Colaborador",
            yaxis_title="Pontuação",
            xaxis_tickangle=-45,      # Angle labels if many collaborators shown
            height=400,
            margin=dict(l=10, r=10, t=10, b=100) # Increase bottom margin for angled labels
        )
        st.plotly_chart(fig_bar_rank, use_container_width=True)
    else:
        st.info("Ainda não há dados de pontuação para exibir.")
    st.divider()

    # --- Análise por Cliente ---
    st.subheader("📊 Análise por Cliente")
    client_for_analysis = selected_client_name_filter if selected_client_name_filter != "Todos" else None

    if client_for_analysis:
        st.info(f"**Cliente Selecionado:** {client_for_analysis}")

        # Fetch analysis data using the new method
        # Filter by collaborator if the current role is Usuario
        collab_filter_for_analysis = username if role == 'Usuario' else None
        analysis_data = manager.get_analise_cliente_data_local(client_for_analysis, collab_filter_for_analysis)

        col_an1, col_an2 = st.columns(2)

        with col_an1: # Left side - Published vs Pending Donut
            st.markdown("**Status Geral**")
            docs_drive = analysis_data['docs_no_drive']
            docs_pub = analysis_data['docs_publicados']
            docs_pend = analysis_data['docs_pendentes']
            st.markdown(f"🟢 Documentos no Drive - **{docs_drive}** (Meta)") # Indicate it's a target
            st.markdown(f"🔵 Documentos Publicados - **{docs_pub}**")
            st.markdown(f"🔴 Documentos Pendentes - **{docs_pend}**")

            labels_status = ['Publicados', 'Pendentes']
            values_status = [docs_pub, docs_pend]
            colors_status = ['#1f77b4', '#d62728'] # Blue, Red approx.

            if sum(values_status) > 0 or docs_drive > 0: # Show if target exists even if no docs yet
                fig_donut_status = go.Figure(data=[go.Pie(labels=labels_status,
                                                        values=values_status,
                                                        hole=.4,
                                                        marker_colors=colors_status,
                                                        pull=[0.02, 0.02],
                                                        sort=False # Keep order Pub, Pend
                                                        )])
                fig_donut_status.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10, l=10, r=10))
                st.plotly_chart(fig_donut_status, use_container_width=True)
            else:
                st.caption("Nenhum documento para análise de status.")


        with col_an2: # Right side - Criteria Donut
            st.markdown("**Documentos por Critério**")
            crit_counts = analysis_data.get('criterios_counts', {})

            labels_crit = []
            values_crit = []
            colors_crit = []

            # Use defined criteria order and colors
            for crit_name, color in config.CRITERIA_COLORS.items():
                 count = crit_counts.get(crit_name, 0)
                 st.markdown(f'<span style="color:{color}; font-size: 1.1em;">■</span> {crit_name} - **{count}**', unsafe_allow_html=True)
                 if count > 0: # Only add to chart if count > 0
                     labels_crit.append(crit_name)
                     values_crit.append(count)
                     colors_crit.append(color)


            if sum(values_crit) > 0:
                fig_donut_crit = go.Figure(data=[go.Pie(labels=labels_crit,
                                                         values=values_crit,
                                                         hole=.4,
                                                         marker_colors=colors_crit,
                                                         pull=[0.02] * len(labels_crit) # Espaço entre fatias
                                                         )])
                fig_donut_crit.update_layout(showlegend=False, height=300, margin=dict(t=10, b=10, l=10, r=10))
                st.plotly_chart(fig_donut_crit, use_container_width=True)
            else:
                 st.caption("Nenhum documento classificado por critério.")


    else:
        st.info("⬅️ Selecione um cliente na barra lateral para ver a análise detalhada.")

else:
    st.error("Perfil de usuário desconhecido.")