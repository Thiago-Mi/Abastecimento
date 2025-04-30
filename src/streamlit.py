# --- START OF FILE streamlit.py ---
import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os
# Removido StringIO, BytesIO daqui - estão em file_processors
# from io import StringIO, BytesIO

# Importações dos novos módulos
import config
from db import GerenciadorBD, Autenticador # DB_FILE não é mais necessário aqui
from file_processors import (
    processar_arquivo_txt_usuarios,
    processar_arquivo_clientes,
    processar_arquivo_associacoes
)


class AppStreamlit:
    """Controla a interface e o fluxo da aplicação Streamlit."""

    def __init__(self): # Não precisa mais receber db_file aqui
        """Inicializa a aplicação, o gerenciador de BD e o autenticador."""
        # Usa DB_FILE do config
        self.gerenciador_bd = GerenciadorBD(config.DB_FILE)
        self.autenticador = Autenticador(self.gerenciador_bd)

        # Inicialização do session_state (sem mudanças)
        if 'logged_in' not in st.session_state: 
            st.session_state['logged_in'] = False
            
        default_states = {
            'username': None, 'role': None, 'nome_completo': None,
            'selected_collaborator': None, 'selected_client_type': None, 'selected_client_id': None,
            'cliente_id': None, 'cliente_nome': None, # Adicionado para cliente logado
            'show_add_user_form': False, 'show_add_client_form': False,
            'mass_upload_results': None, 'client_upload_results': None, 'assoc_upload_results': None,
            'selected_period': "Todos" # NOVO: Estado para filtro de período
        }
        for key, value in default_states.items():
            if key not in st.session_state: st.session_state[key] = value

    def _mostrar_tela_login(self):
        """Exibe o formulário de login centralizado."""
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            # Usa LOGO_PATH do config
            if os.path.exists(config.LOGO_PATH):
                st.image(config.LOGO_PATH, width=300)
            else:
                st.warning(f"Logo não encontrado em: {os.path.abspath(config.LOGO_PATH)}")

            # Usa APP_TITLE do config
            st.header(f"Login - {config.APP_TITLE}")
            with st.form("login_form"):
                username = st.text_input("Usuário")
                password = st.text_input("Senha", type="password")
                submitted = st.form_submit_button("Entrar")
                if submitted:
                    # Limpa TUDO no login para um estado inicial limpo
                    current_keys = list(st.session_state.keys())
                    for key in current_keys:
                        del st.session_state[key]
                    # Tenta logar (isso vai setar logged_in etc. se sucesso)
                    success, message = self.autenticador.login(username, password)
                    if not success:
                        st.error(message)
                        # Se falhar, precisa garantir que logged_in é False
                        st.session_state['logged_in'] = False
                    # Não precisa de st.rerun explícito aqui

    # REMOVIDOS os métodos _processar_arquivo... daqui
    # def _processar_arquivo_txt_usuarios(...)
    # def _processar_arquivo_clientes(...)
    # def _processar_arquivo_associacoes(...)


    def _mostrar_sidebar(self):
        """Exibe a barra lateral apropriada para o role do usuário."""
        st.sidebar.image(config.LOGO_PATH, use_container_width=True)
        st.sidebar.divider()

        role = st.session_state.get('role')

        # --- Sidebar para Cliente ---
        if role == 'Cliente':
            st.sidebar.subheader("Filtros")
            period_options = ["Todos", "Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias", "Este Mês"]
            # Mantém a seleção atual ou define "Todos"
            current_period = st.session_state.get('selected_period', "Todos")
            if current_period not in period_options: current_period = "Todos" # Fallback
            index_period = period_options.index(current_period)

            selected_period_label = st.sidebar.selectbox(
                "Selecione o Período:",
                period_options,
                index=index_period,
                key='client_period_select'
            )
            # Atualiza o estado se a seleção mudar
            if st.session_state.get('selected_period') != selected_period_label:
                st.session_state['selected_period'] = selected_period_label
                st.rerun() # Precisa recarregar o painel com o novo período

            st.sidebar.divider()
            st.sidebar.info(f"Logado como: {st.session_state.get('cliente_nome', st.session_state.get('nome_completo','N/A'))}")
            st.sidebar.caption(f"Perfil: {role}")


        # --- Sidebar para Admin/Usuario ---
        elif role in ['Admin', 'Usuario']:
            # --- Filtros (Admin/Usuario) ---
            colaboradores = self.gerenciador_bd.listar_colaboradores()
            # ... (resto da lógica de filtros para Admin/Usuario com os st.rerun necessários) ...
            map_colaborador_nome = {c['nome_completo']: c['username'] for c in colaboradores}
            map_colaborador_username = {c['username']: c['nome_completo'] for c in colaboradores}
            is_admin = st.session_state['role'] == 'Admin'
            # 1. Select Colaborador (Admin only)
            if is_admin:
                opcoes_colaborador = ["Todos"] + list(map_colaborador_nome.keys())
                # ... (lógica selectbox colab com rerun) ...
                current_colab_name = map_colaborador_username.get(st.session_state.get('selected_collaborator'))
                index_colab = opcoes_colaborador.index(current_colab_name) if current_colab_name in opcoes_colaborador else 0
                colab_selecionado_nome = st.sidebar.selectbox("Selecione Colaborador:", opcoes_colaborador, index=index_colab, key='select_colab')
                new_username = map_colaborador_nome.get(colab_selecionado_nome) if colab_selecionado_nome != "Todos" else None
                if st.session_state.get('selected_collaborator') != new_username:
                    st.session_state['selected_collaborator'] = new_username; st.session_state['selected_client_type'] = None; st.session_state['selected_client_id'] = None
                    st.rerun()
            else: # Usuario
                st.sidebar.write("Colaborador:"); st.sidebar.info(st.session_state['nome_completo'])
                if st.session_state.get('selected_collaborator') != st.session_state['username']:
                     st.session_state['selected_collaborator'] = st.session_state['username']; st.session_state['selected_client_type'] = None; st.session_state['selected_client_id'] = None


            colaborador_filtrado = st.session_state.get('selected_collaborator')
            # 2. Select Tipo Cliente
            tipos_cliente = self.gerenciador_bd.listar_tipos_cliente(colaborador_username=colaborador_filtrado)
            # ... (lógica selectbox tipo com rerun) ...
            opcoes_tipo = ["Todos"] + tipos_cliente
            current_type = st.session_state.get('selected_client_type')
            index_type = opcoes_tipo.index(current_type) if current_type in opcoes_tipo else 0
            selected_type = st.sidebar.selectbox("Selecione Tipo de Cliente:", opcoes_tipo, index=index_type, key='select_tipo', disabled=not bool(tipos_cliente))
            type_filter = selected_type if selected_type != "Todos" else None
            if st.session_state.get('selected_client_type') != type_filter:
                 st.session_state['selected_client_type'] = type_filter; st.session_state['selected_client_id'] = None
                 st.rerun()

            tipo_cliente_filtrado = st.session_state.get('selected_client_type')
            # 3. Select Cliente
            clientes = self.gerenciador_bd.listar_clientes(colaborador_username=colaborador_filtrado, tipo_cliente=tipo_cliente_filtrado)
            # ... (lógica selectbox cliente com rerun) ...
            map_cliente_nome_id = {c['nome']: c['id'] for c in clientes}
            map_cliente_id_nome = {c['id']: c['nome'] for c in clientes}
            opcoes_cliente = ["Selecione..."] + list(map_cliente_nome_id.keys())
            current_client_name = map_cliente_id_nome.get(st.session_state.get('selected_client_id'))
            index_cliente = opcoes_cliente.index(current_client_name) if current_client_name in opcoes_cliente else 0
            selected_client_name = st.sidebar.selectbox("Selecione Cliente:", opcoes_cliente, index=index_cliente, key='select_cliente', disabled=not bool(clientes))
            new_client_id = map_cliente_nome_id.get(selected_client_name) if selected_client_name != "Selecione..." else None
            if st.session_state.get('selected_client_id') != new_client_id:
                 st.session_state['selected_client_id'] = new_client_id
                 st.rerun()


        # --- Ações do Administrador ---
            st.sidebar.divider()
            if is_admin:
                st.sidebar.subheader("Cadastros Individuais")
                if st.sidebar.button("➕ Usuário/Colab.", key='btn_add_user'):
                    st.session_state.update({'show_add_user_form': not st.session_state.get('show_add_user_form', False),
                                            'show_add_client_form': False, 'mass_upload_results': None,
                                            'client_upload_results': None, 'assoc_upload_results': None}) 
                    st.rerun()
                if st.sidebar.button("🏢 Cliente", key='btn_add_client'):
                    st.session_state.update({'show_add_client_form': not st.session_state.get('show_add_client_form', False),
                                            'show_add_user_form': False, 'mass_upload_results': None,
                                            'client_upload_results': None, 'assoc_upload_results': None}) 
                    st.rerun()


                st.sidebar.divider()
                st.sidebar.subheader("Cadastros em Massa")

                # Upload TXT Usuários
                st.sidebar.caption("Usuários (.txt): `usuario,nome,senha,Role`")
                uploaded_user_txt = st.sidebar.file_uploader("Upload Usuários (TXT)", type="txt", key="mass_upload_user")
                if st.sidebar.button("⚙️ Processar TXT Usuários", key="btn_process_user_txt"):
                    if uploaded_user_txt:
                        # Chama a função importada
                        results = processar_arquivo_txt_usuarios(self.gerenciador_bd, uploaded_user_txt)
                        st.session_state.update({'mass_upload_results': results, 'show_add_user_form': False, 'show_add_client_form': False,
                                                'client_upload_results': None, 'assoc_upload_results': None})
                        st.rerun() # Necessário para exibir resultados
                    else: st.sidebar.warning("Selecione um arquivo TXT.")

                # Upload Clientes CSV/XLSX
                st.sidebar.caption("Clientes (.csv/.xlsx): Colunas 'nome', 'tipo'")
                uploaded_clients_file = st.sidebar.file_uploader("Upload Clientes (CSV/XLSX)", type=['csv', 'xlsx'], key="mass_upload_client")
                if st.sidebar.button("⚙️ Processar Arquivo Clientes", key="btn_process_client_file"):
                    if uploaded_clients_file:
                        # Chama a função importada
                        results = processar_arquivo_clientes(self.gerenciador_bd, uploaded_clients_file)
                        st.session_state.update({'client_upload_results': results, 'show_add_user_form': False, 'show_add_client_form': False,
                                                'mass_upload_results': None, 'assoc_upload_results': None})
                        st.rerun() # Necessário para exibir resultados
                    else: st.sidebar.warning("Selecione um arquivo CSV/XLSX.")

                # Upload Associações CSV/XLSX
                st.sidebar.caption("Associações (.csv/.xlsx): 'colaborador_username', 'cliente_nome'")
                uploaded_assoc_file = st.sidebar.file_uploader("Upload Associações (CSV/XLSX)", type=['csv', 'xlsx'], key="mass_upload_assoc")
                if st.sidebar.button("⚙️ Processar Arquivo Associações", key="btn_process_assoc_file"):
                    if uploaded_assoc_file:
                        # Chama a função importada
                        results = processar_arquivo_associacoes(self.gerenciador_bd, uploaded_assoc_file)
                        st.session_state.update({'assoc_upload_results': results, 'show_add_user_form': False, 'show_add_client_form': False,
                                                'mass_upload_results': None, 'client_upload_results': None})
                        st.rerun() # Necessário para exibir resultados
                    else: st.sidebar.warning("Selecione um arquivo CSV/XLSX.")

            # --- Informações do Usuário e Logout ---
            # ... (permanece igual) ...
            st.sidebar.divider()
            st.sidebar.info(f"Logado: {st.session_state.get('nome_completo', 'N/A')}")
            st.sidebar.caption(f"Papel: {st.session_state.get('role', 'N/A')}")
        if st.sidebar.button("Logout"): 
            self.autenticador.logout()

        return st.session_state.get('selected_collaborator'), st.session_state.get('selected_client_id')


    def _display_upload_results(self, results_key, title):
        """Função auxiliar para exibir resultados de upload e limpar o estado."""
        if results_key in st.session_state and st.session_state[results_key]:
            results = st.session_state[results_key]
            st.subheader(f"Resultados: {title}")
            with st.container(border=True):
                if results.get('failed', 0) == -1: # Erro geral de leitura/formato
                     st.error(f"Erro ao processar o arquivo: {' '.join(results.get('errors', ['Erro desconhecido.']))}")
                else:
                    st.success(f"{results.get('success', 0)} registros processados com sucesso.")
                    if results.get('failed', 0) > 0:
                        st.error(f"{results['failed']} registros falharam.")
                        with st.expander("Ver detalhes dos erros"):
                            st.code('\n'.join(results.get('errors', [])), language=None)
            # Limpa os resultados após exibir
            st.session_state[results_key] = None
            st.markdown("---")
            return True # Indica que resultados foram mostrados
        return False # Indica que não havia resultados para mostrar


    def _mostrar_dashboard_comum(self, colaborador_selecionado, cliente_selecionado_id):
        """Exibe os elementos comuns do dashboard com layout e dados ajustados."""

        # --- Título Principal ---
        # O ícone pode ser um emoji ou um caractere unicode. FontAwesome não é nativo.
        # O nome do usuário logado já está na sidebar.
        st.markdown("#### 🗓️ Acompanhamento Abastecimento - Atricon 2025")
        st.write("") # Espaçamento

        # --- KPIs (Cards) ---
        kpi_data = self.gerenciador_bd.get_kpi_data(colaborador_username=colaborador_selecionado)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Links Enviados", f"{kpi_data.get('enviados', 0):02d}") # :02d para formatar com 2 dígitos
        with col2:
            st.metric("Links Validados", f"{kpi_data.get('validados', 0):02d}")
        with col3:
            # Nome na imagem é "Links Pendentes", pode ou não incluir os enviados
            # Usaremos a contagem direta do status 'Pendente' por enquanto.
            st.metric("Links Pendentes", f"{kpi_data.get('pendentes', 0):02d}")
        with col4:
            st.metric("Links Inválidos", f"{kpi_data.get('invalidos', 0):02d}")

        st.divider()

        # --- Gráfico de Barras (Ranking por Pontuação) ---
        st.subheader("🏆 Ranking de Colaboradores por Pontuação")
        df_pontuacao = self.gerenciador_bd.calcular_pontuacao_colaboradores()

        if not df_pontuacao.empty:
            # Prepara dados para gráfico
            df_display = df_pontuacao.head(15) # Limita a exibição para clareza
            df_display = df_display.sort_values(by='Pontuação', ascending=True) # Melhor para bar chart horizontal ou se não couberem todos

            # Cria rótulos como na imagem (Count (Percentage%))
            # Usa a coluna 'Links Validados' e 'Percentual' calculada no DB
            labels = [f"{row['Links Validados']} ({row['Percentual']:.1f}%)" for index, row in df_display.iterrows()]

            # Define cores das barras (destaca o maior)
            colors = [config.DEFAULT_BAR_COLOR] * len(df_display)
            if not df_display.empty:
                max_score_index = df_display['Pontuação'].idxmax() # Nome do colaborador com maior pontuação
                try:
                     idx_pos = df_display.index.get_loc(max_score_index)
                     colors[idx_pos] = config.HIGHLIGHT_BAR_COLOR
                except KeyError:
                     print(f"Warning: Colaborador {max_score_index} não encontrado no índice do dataframe de display.")


            fig_bar = go.Figure(go.Bar(
                x=df_display.index,       # Colaborador no eixo X
                y=df_display['Pontuação'], # Pontuação no eixo Y
                # orientation='h',        # REMOVIDO - padrão é vertical
                text=labels,              # Rótulos personalizados (mantém)
                textposition='outside',   # Posição do texto (pode ajustar se necessário, e.g., 'auto')
                marker_color=colors       # Cores personalizadas (mantém)
            ))

            fig_bar.update_layout(
                title="Pontuação e % Links Validados por Colaborador",
                xaxis_title="Colaborador",                             # Trocado
                yaxis_title="Pontuação (10 pts/link validado)",        # Trocado
                xaxis={'categoryorder':'total descending'},            # Ordena o eixo X pelo valor Y (descendente)
                height=max(400, len(df_display)*35), # Pode precisar ajustar a altura/largura para vertical
                margin=dict(l=10, r=10, t=20, b=5) # Mantém margens
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Ainda não há dados de pontuação para exibir.")

        st.divider()

        # --- Análise por Cliente ---
        cliente_info = self.gerenciador_bd.buscar_cliente_por_id(cliente_selecionado_id) if cliente_selecionado_id else None
        titulo_analise = "📊 Análise por Cliente"
        if cliente_info:
            titulo_analise += f" - {cliente_info['nome']}"
        st.subheader(titulo_analise)


        if cliente_selecionado_id: # Mantém a lógica original para exibir a análise
            # Verifica se cliente_info foi realmente encontrado (embora deva ser, se ID existe)
            if not cliente_info:
                st.warning(f"Cliente com ID {cliente_selecionado_id} não encontrado no banco de dados.")
                return # Ou st.stop() dependendo do fluxo desejado

            analise_data = self.gerenciador_bd.get_analise_cliente_data(
                cliente_id=cliente_selecionado_id,
                colaborador_username=colaborador_selecionado if st.session_state['role'] == 'Usuario' else None
            )

            col_analise1, col_analise2 = st.columns(2)

            # Coluna Esquerda: Status Geral
            with col_analise1:
                st.markdown("**Status Geral de Documentos**")
                # Legenda com emojis
                st.markdown(f"🟢 Documentos no Drive - **{analise_data['docs_drive']}**")
                st.markdown(f"🔵 Documentos Publicados - **{analise_data['docs_publicados']}**")
                st.markdown(f"🔴 Documentos Pendentes - **{analise_data['docs_pendentes']}**")

                # Gráfico Donut 1: Publicados vs Pendentes
                labels_status = ['Publicados', 'Pendentes']
                values_status = [analise_data['docs_publicados'], analise_data['docs_pendentes']]
                colors_status = ['#1f77b4', '#d62728'] # Azul e Vermelho approx.

                if sum(values_status) > 0:
                    fig_donut_status = go.Figure(data=[go.Pie(labels=labels_status,
                                                            values=values_status,
                                                            hole=.4,
                                                            marker_colors=colors_status,
                                                            pull=[0.02, 0.02] # Pequeno espaço entre fatias
                                                            )])
                    fig_donut_status.update_layout(showlegend=False, height=350, margin=dict(t=30, b=10, l=10, r=10))
                    st.plotly_chart(fig_donut_status, use_container_width=True)
                else:
                    st.caption("Nenhum documento publicado ou pendente.")


            # Coluna Direita: Critérios
            with col_analise2:
                st.markdown("**Documentos por Critério**")
                crit_counts = analise_data.get('criterios_counts', {})
                # Legenda com emojis e dados
                st.markdown(f"🟩 Critérios Essenciais - **{crit_counts.get('Critérios Essenciais', 0)}**")
                st.markdown(f"🟧 Critérios Obrigatórios - **{crit_counts.get('Obrigatórios', 0)}**")
                st.markdown(f"🟨 Critérios Recomendados - **{crit_counts.get('Recomendados', 0)}**")

                # Gráfico Donut 2: Critérios
                labels_criterios = list(crit_counts.keys())
                values_criterios = list(crit_counts.values())
                # Cores aproximadas da imagem
                colors_criterios = ['#2ca02c', '#ff7f0e', '#ffdd71'] # Verde, Laranja, Amarelo

                # Remover critérios com contagem 0 para não poluir o gráfico
                valid_indices = [i for i, v in enumerate(values_criterios) if v > 0]
                labels_criterios = [labels_criterios[i] for i in valid_indices]
                values_criterios = [values_criterios[i] for i in valid_indices]
                colors_criterios = [colors_criterios[i] for i in valid_indices]


                if sum(values_criterios) > 0:
                    fig_donut_criterios = go.Figure(data=[go.Pie(labels=labels_criterios,
                                                                 values=values_criterios,
                                                                 hole=.4,
                                                                 marker_colors=colors_criterios,
                                                                 pull=[0.02] * len(values_criterios) # Espaço entre fatias
                                                                 )])
                    fig_donut_criterios.update_layout(showlegend=False, height=350, margin=dict(t=30, b=10, l=10, r=10))
                    st.plotly_chart(fig_donut_criterios, use_container_width=True)
                else:
                     st.caption("Nenhum documento classificado por critério.")

        else:
            st.info("⬅️ Selecione um cliente na barra lateral para ver a análise detalhada.")


    def _mostrar_painel_admin(self):
        """Exibe o painel do Administrador."""
        colaborador_filtro, cliente_filtro_id = self._mostrar_sidebar()

        # Usa o helper para exibir resultados (permanece método da classe)
        results_shown = False
        results_shown |= self._display_upload_results('mass_upload_results', "Cadastro Usuários (TXT)")
        results_shown |= self._display_upload_results('client_upload_results', "Cadastro Clientes (Arquivo)")
        results_shown |= self._display_upload_results('assoc_upload_results', "Associações Colab./Cliente (Arquivo)")

        # Formulários Individuais (permanecem como parte do painel admin)
        user_form_active = False
        if st.session_state.get('show_add_user_form'):
             user_form_active = True
             st.subheader("Cadastrar Usuário Individual")
             with st.container(border=True):
                  with st.form("new_user_form", clear_on_submit=True):
                    new_username = st.text_input("Novo Nome de Usuário (Login)", key="nu_uname")
                    new_fullname = st.text_input("Nome Completo", key="nu_fname")
                    new_password = st.text_input("Nova Senha", type="password", key="nu_pass")
                    new_role = st.selectbox("Papel (Role)", ["Admin", "Usuario", "Cliente"], key="nu_role")
                    submit_new_user = st.form_submit_button("Cadastrar Usuário")
                    if submit_new_user:
                        if new_username and new_fullname and new_password and new_role:
                            success, message = self.gerenciador_bd.adicionar_usuario(new_username, new_password, new_fullname, new_role)
                            if success: st.success(message); #st.rerun()
                            else: st.error(message)
                        else: st.warning("Por favor, preencha todos os campos.")

        client_form_active = False
        if st.session_state.get('show_add_client_form'):
             client_form_active = True
             st.subheader("Cadastrar Cliente Individual")
             with st.container(border=True):
                 with st.form("new_client_form", clear_on_submit=True):
                    new_client_name = st.text_input("Nome do Cliente", key="nc_name")
                    tipos_existentes = self.gerenciador_bd.listar_tipos_cliente()
                    tipos_opcao = sorted(list(set(["Prefeitura", "Câmara", "Autarquia", "Outro"] + tipos_existentes)))
                    new_client_type = st.selectbox("Tipo de Cliente", tipos_opcao, key="nc_type")
                    submit_new_client = st.form_submit_button("Cadastrar Cliente")
                    if submit_new_client:
                        if new_client_name and new_client_type:
                            success, message = self.gerenciador_bd.adicionar_cliente(new_client_name, new_client_type)
                            if success: st.success(message); #st.rerun()
                            else: st.error(message)
                        else: st.warning("Por favor, preencha todos os campos.")

        # Adiciona separador apenas se houver forms individuais ativos E nenhum resultado de upload foi mostrado antes
        if (user_form_active or client_form_active) and not results_shown:
            st.markdown("---")

        # --- Dashboard Principal ---
        self._mostrar_dashboard_comum(colaborador_filtro, cliente_filtro_id)


    def _mostrar_painel_usuario(self):
         _, cliente_filtro_id = self._mostrar_sidebar()
         self._mostrar_dashboard_comum(st.session_state['username'], cliente_filtro_id)

    def _mostrar_painel_cliente(self):
        """Exibe o dashboard específico para o cliente logado."""
        self._mostrar_sidebar() # Mostra a sidebar simplificada

        cliente_id = st.session_state.get('cliente_id')
        cliente_nome = st.session_state.get('cliente_nome', "Cliente")

        # Header
        col_h1, col_h2 = st.columns([0.9, 0.1])
        with col_h1:
             st.markdown(f"#### 🗓️ Acompanhamento Abastecimento - Atricon 2025")
        with col_h2:
             st.caption(f"👤 {cliente_nome}") # Mostra nome do cliente
        st.write("")

        # Filtro de Período selecionado na Sidebar
        periodo_selecionado = st.session_state.get('selected_period', "Todos")
        periodo_dias_map = {
             "Últimos 7 dias": 7, "Últimos 30 dias": 30, "Últimos 90 dias": 90,
             "Este Mês": 30 # Aproximação, lógica mais precisa seria necessária
        }
        periodo_dias = periodo_dias_map.get(periodo_selecionado) # Será None se for "Todos"

        if cliente_id is None:
            st.error("Não foi possível identificar o cliente associado a este usuário.")
            return

        # KPIs do Cliente
        kpi_data = self.gerenciador_bd.get_kpi_data_cliente(cliente_id, periodo_dias)
        kpi_cols = st.columns(3)
        with kpi_cols[0]:
             st.metric("Docs Enviados", f"{kpi_data.get('enviados', 0):02d}")
        with kpi_cols[1]:
             st.metric("Docs Publicados", f"{kpi_data.get('publicados', 0):02d}") # 'Publicados' = Validados?
        with kpi_cols[2]:
             st.metric("Docs Pendentes", f"{kpi_data.get('pendentes', 0):02d}") # 'Pendentes' = Status Pendente?

        st.divider()

        # Gráfico de Linha
        st.subheader("📈 Documentos Publicados por Período")
        # Define agrupamento com base na seleção (poderia ser mais sofisticado)
        grupo_tempo = 'W' # Padrão Semanal
        if periodo_dias and periodo_dias <= 31:
             grupo_tempo = 'D' # Dias se período for curto
        elif periodo_dias and periodo_dias > 90:
              grupo_tempo = 'M' # Meses se período for longo

        df_line = self.gerenciador_bd.get_docs_por_periodo_cliente(cliente_id, grupo=grupo_tempo)

        if not df_line.empty and 'periodo_dt' in df_line.columns:
             # Usar a coluna de data convertida para o eixo X
             fig_line = px.line(df_line, x='periodo_dt', y='contagem', markers=True,
                                 labels={'periodo_dt': 'Período', 'contagem': 'Documentos Publicados'})
             fig_line.update_layout(
                 xaxis_title="Data",
                 yaxis_title="Quantidade",
                 height=300,
                 margin=dict(l=10, r=10, t=10, b=10)
             )
             # Adicionar anotação do pico se desejado (requer identificar o pico)
             if not df_line.empty:
                 peak_idx = df_line['contagem'].idxmax()
                 peak_row = df_line.loc[peak_idx]
                 fig_line.add_annotation(x=peak_row['periodo_dt'], y=peak_row['contagem'],
                                         text=f"{peak_row['contagem']}", showarrow=True, arrowhead=1,
                                         bgcolor="blue", font=dict(color="white"))

             st.plotly_chart(fig_line, use_container_width=True)
        else:
             st.info("Não há dados suficientes para exibir o gráfico de linha neste período.")


        st.divider()

        # Critérios Atendidos
        st.subheader("📊 Critérios Atendidos")
        crit_data = self.gerenciador_bd.get_criterios_atendidos_cliente(cliente_id)

        if not crit_data:
            st.info("Nenhum critério encontrado para este cliente.")
        else:
            for criterio, data in crit_data.items():
                total = data['total']
                atendidos = data['atendidos']
                percentual = (atendidos / total * 100) if total > 0 else 0

                # Layout: [Cor] [Nome Criterio] [Barra de Progresso] [Texto Contagem/Percentual]
                col_cor, col_nome, col_barra, col_texto = st.columns([0.05, 0.25, 0.4, 0.3])

                with col_cor:
                    # Simula a cor com um emoji ou caractere colorido (requer ajuste de CSS ou imagem para cor real)
                    color = config.CRITERIA_COLORS.get(criterio, config.DEFAULT_CRITERIA_COLOR)
                    st.markdown(f'<span style="color:{color}; font-size: 1.5em;">■</span>', unsafe_allow_html=True)
                with col_nome:
                    st.write(criterio)
                with col_barra:
                    st.progress(percentual / 100) # Barra de progresso espera valor 0-1
                with col_texto:
                    st.markdown(f"  {atendidos} docs / {percentual:.0f}%") # Usa markdown para alinhar

    def run(self):
        """Executa o fluxo principal da aplicação."""
        # Garante que o estado inicial seja definido se não logado
        if not st.session_state.get('logged_in'):
             self._mostrar_tela_login()
             # Importante: Não executa o resto do código se não estiver logado
             # Garante que a limpeza no login funcione corretamente antes de tentar renderizar painéis
             return

        # Roteamento normal se logado
        role = st.session_state['role']
        if role == 'Admin': self._mostrar_painel_admin()
        elif role == 'Usuario': self._mostrar_painel_usuario()
        elif role == 'Cliente': self._mostrar_painel_cliente()
        elif role: # Se logado mas com role desconhecido
             st.error("Papel de usuário desconhecido. Fazendo logout.")
             self.autenticador.logout()
        # Se chegou aqui sem role (improvável se logged_in é True, mas por segurança)
        # else: self._mostrar_tela_login() # Ou força logout

# --- Ponto de Entrada da Aplicação ---
if __name__ == "__main__":
    # Usa constantes do config
    st.set_page_config(
        page_title=config.APP_TITLE,
        page_icon=config.LOGO_PATH if os.path.exists(config.LOGO_PATH) else "📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Instancia AppStreamlit sem passar o db_file
    app = AppStreamlit()
    app.run()

# --- END OF FILE streamlit.py ---