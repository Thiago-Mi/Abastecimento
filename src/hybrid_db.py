# hybrid_db.py
import streamlit as st
import pandas as pd
import sqlite3
import gspread
from datetime import datetime
import hashlib
import uuid # For generating unique IDs for documents

import config
import sheets_auth # Our authentication module

class HybridDBManager:  
    """
    Manages data synchronization between Google Sheets (master) and a local
    in-memory SQLite database (session cache).
    """
    def __init__(self):
        """Initializes the manager, gets gspread client, connects to local DB."""
        self.gc = sheets_auth.get_gspread_client()
        try:
            print("abrindo")
            self.spreadsheet = self.gc.open_by_url(config.GOOGLE_SHEET_URL)
        except Exception as e:
            st.error(f"Failed to open Google Sheet '{config.GOOGLE_SHEET_URL}': {e}")
            st.stop()

        # Connect to in-memory SQLite database for the session
        self.local_conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.local_conn.row_factory = sqlite3.Row # Return dict-like rows
        print("Connected to local in-memory SQLite DB.")
        self._create_local_tables()

    def _execute_local_sql(self, query, params=None, fetch_mode="all"):
        """Helper to execute SQL on the local SQLite DB."""
        cursor = self.local_conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith("SELECT"):
                if fetch_mode == "one":
                    return cursor.fetchone()
                elif fetch_mode == "all":
                    return cursor.fetchall()
                else: # No fetch needed for INSERT/UPDATE/DELETE etc.
                     self.local_conn.commit()
                     return cursor.rowcount
            else: # For INSERT, UPDATE, DELETE
                self.local_conn.commit()
                return cursor.rowcount
        except sqlite3.Error as e:
            st.error(f"Local SQLite Error: {e}\nQuery: {query[:100]}...")
            print(f"Local SQLite Error: {e}\nQuery: {query}\nParams: {params}")
            # In production, you might want more robust error handling
            # Depending on the error, you might want to return None, [], or raise it
            return None # Or raise e
        finally:
             # Cursors are usually lightweight, but closing is good practice
             # Be careful if multiple operations need the same cursor
             pass # Keep connection open for the session


    def _create_local_tables(self):
        """Creates the necessary tables in the local in-memory SQLite DB."""
        print("Creating local SQLite tables...")
        # --- Usuarios Table ---
        # Matching columns from config.USERS_COLS potentially + local needs
        self._execute_local_sql("""
            CREATE TABLE IF NOT EXISTS usuarios (
                username TEXT PRIMARY KEY UNIQUE NOT NULL,
                hashed_password TEXT,
                nome_completo TEXT,
                role TEXT CHECK(role IN ('Admin', 'Usuario', 'Cliente')),
                last_sync_timestamp TEXT
            )
        """)

        # --- Clientes Table ---
        self._execute_local_sql("""
            CREATE TABLE IF NOT EXISTS clientes (
                id TEXT PRIMARY KEY UNIQUE NOT NULL, -- Assuming UUID or unique identifier from Sheet
                nome TEXT UNIQUE NOT NULL,
                tipo TEXT
            )
        """)

        # --- Associações Table (Optional) ---
        self._execute_local_sql("""
           CREATE TABLE IF NOT EXISTS colaborador_cliente (
               colaborador_username TEXT NOT NULL,
               cliente_nome TEXT NOT NULL,
               PRIMARY KEY (colaborador_username, cliente_nome) -- Chave primária composta
           )
        """)
        print("Local SQLite tables created.")

        
        
        # --- Documentos Table (Merged) ---
        cols_config = config.DOCS_COLS
        cols_sql = ", ".join([f'"{col}" TEXT' for col in cols_config])
        cols_sql = cols_sql.replace('"id" TEXT', '"id" TEXT PRIMARY KEY') # Mantém id como PK

        # Adiciona a nova coluna local
        create_docs_sql = f"""
            CREATE TABLE IF NOT EXISTS documentos (
                {cols_sql},
                is_synced INTEGER DEFAULT 0 NOT NULL -- 0 = não sincronizado, 1 = sincronizado/carregado do GSheet
            )
        """
        self._execute_local_sql(create_docs_sql)
        print("Local SQLite tables created (incluindo tabela de documentos com is_synced).") # Mensagem única no final

    def _get_worksheet(self, sheet_name):
        """Safely gets a worksheet, returns None if not found."""
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found.")
            return None
        except Exception as e:
            st.error(f"Error accessing worksheet '{sheet_name}': {e}")
            return None

    def _load_sheet_to_local_table(self, sheet_name, table_name, expected_cols, if_exists='replace'):
        """Loads data from a GSheet worksheet into a local SQLite table."""
        ws = self._get_worksheet(sheet_name)
        if not ws:
            # Só mostra warning se a tabela não for opcional (como docs de usuário)
            if table_name not in ["documentos", "colaborador_cliente"]: # Não mostrar para essas que podem não existir ainda
                 st.warning(f"Skipping load for non-existent sheet: {sheet_name}")
            else:
                 print(f"Sheet '{sheet_name}' not found, skipping load into '{table_name}'.")
            return True # Considera "sucesso" pois a planilha pode não existir

        print(f"Loading data from GSheet '{sheet_name}' to local table '{table_name}' (mode: {if_exists})...")
        try:
            # Usar get_values para não depender da primeira linha ser cabeçalho consistente
            # E para funcionar com planilhas vazias
            all_values = ws.get_values()
            if len(all_values) < 1: # Vazia ou sem cabeçalho
                print(f"Sheet '{sheet_name}' is empty or has no header.")
                if if_exists == 'replace':
                     self._execute_local_sql(f"DELETE FROM {table_name}")
                return True

            header = all_values[0]
            data = all_values[1:]

            if not data: # Apenas cabeçalho
                print(f"Sheet '{sheet_name}' has only a header.")
                if if_exists == 'replace':
                     self._execute_local_sql(f"DELETE FROM {table_name}")
                return True

            df = pd.DataFrame(data, columns=header)

            # --- Column Validation/Alignment ---
            # Usar as colunas ESPERADAS do config
            actual_header = df.columns.tolist()
            df_selected = pd.DataFrame(columns=expected_cols) # Df vazio com colunas esperadas

            cols_to_copy = [col for col in expected_cols if col in actual_header]
            if not cols_to_copy:
                 print(f"WARNING: No expected columns found in sheet '{sheet_name}'. Expected: {expected_cols}, Found: {actual_header}")
                 if if_exists == 'replace': self._execute_local_sql(f"DELETE FROM {table_name}")
                 return True # Continua, mas a tabela ficará vazia


            df_selected[cols_to_copy] = df[cols_to_copy] # Copia colunas existentes e esperadas

            # Preenche colunas esperadas mas ausentes no sheet com None/NaN
            missing_cols = [col for col in expected_cols if col not in actual_header]
            for col in missing_cols:
                 df_selected[col] = None

            df = df_selected[expected_cols] # Garante a ordem correta

            df = df.astype(str) # Converte tudo para string para SQLite

            # Insert into SQLite table
            df.to_sql(table_name, self.local_conn, if_exists=if_exists, index=False, chunksize=1000)
            print(f"Successfully loaded {len(df)} rows from '{sheet_name}' to '{table_name}'.")
            return True

        except Exception as e:
            st.error(f"Error loading data from sheet '{sheet_name}': {e}")
            return False

    def _get_user_sheet_name(self, username):
        """Constructs the expected sheet name for a user's documents."""
        return f"{config.USER_DOCS_SHEET_PREFIX}{username}"

    def load_data_for_session(self, username, role):
        """Loads all necessary data from Google Sheets into local SQLite for the session."""
        with st.spinner("Carregando dados da planilha... Por favor, aguarde."):
            print(f"Starting data load for user: {username}, role: {role}")

            # 1. Load Central Sheets (Replace mode)
            load_success = self._load_sheet_to_local_table(config.SHEET_USERS, "usuarios", config.USERS_COLS, if_exists='replace')
            if not load_success: st.stop()
            load_success = self._load_sheet_to_local_table(config.SHEET_CLIENTS, "clientes", config.CLIENTS_COLS, if_exists='replace')
            if not load_success: st.stop()
            # Load associations
            load_success = self._load_sheet_to_local_table(config.SHEET_ASSOC, "colaborador_cliente", config.ASSOC_COLS, if_exists='replace')
            # Não parar se associações falharem, pode ser que não existam ainda. Warning pode ser útil.
            if not load_success: print(f"Warning: Falha ao carregar a planilha de associações '{config.SHEET_ASSOC}'. Funcionalidades de filtro por cliente podem não funcionar.")

            # 2. Load Document Sheets
            # Clear local documents table first before loading potentially multiple sheets
            self._execute_local_sql("DELETE FROM documentos")

            if role == 'Admin':
                print("Admin role detected, loading all user document sheets...")
                users_df = pd.read_sql("SELECT username FROM usuarios WHERE role = 'Usuario'", self.local_conn)
                all_user_sheets_loaded = True
                if not users_df.empty:
                    for user_row in users_df.itertuples():
                        user_sheet_name = self._get_user_sheet_name(user_row.username)
                        # Append data from each user sheet to the *same* local 'documentos' table
                        if not self._load_user_docs_to_local(user_sheet_name):
                              all_user_sheets_loaded = False # Track if any sheet failed
                if not all_user_sheets_loaded:
                     st.warning("Falha ao carregar dados de um ou mais usuários. A visão pode estar incompleta.")

            elif role == 'Usuario':
                user_sheet_name = self._get_user_sheet_name(username)
                print(f"Loading document sheet for user '{username}': {user_sheet_name}")
                if not self._load_user_docs_to_local(user_sheet_name):
                    st.error(f"Não foi possível carregar seus dados de documentos da planilha '{user_sheet_name}'.")
                    # Decide if app should stop or continue with potentially missing data
                    st.stop()
                    
            elif role == 'Cliente':
                # O Cliente não tem uma planilha de docs própria, ele vê dados gerais ou de um cliente específico
                # Precisamos carregar os documentos relacionados ao cliente associado a este usuário.
                # Assumindo que st.session_state['cliente_nome'] foi definido no login
                cliente_nome_logado = st.session_state.get('cliente_nome')
                if not cliente_nome_logado:
                     st.error("Não foi possível determinar o cliente associado a este usuário.")
                     st.stop()

                st.warning("O carregamento de dados para Clientes ainda busca em todas as planilhas de usuário. Otimização necessária para buscar apenas dados relevantes.")
                # TODO: Optimization - This currently loads ALL user sheets even for a Client.
                # A better approach would be to query the 'documentos' sheets only for rows matching
                # the st.session_state['cliente_nome']. This requires more complex gspread filtering
                # or restructuring Sheets data (e.g., all docs in one sheet with client column).
                # For now, we load all and filter locally, which is inefficient for the Client role.
                users_df = pd.read_sql("SELECT username FROM usuarios WHERE role = 'Usuario'", self.local_conn)
                if not users_df.empty:
                    for user_row in users_df.itertuples():
                        user_sheet_name = self._get_user_sheet_name(user_row.username)
                        self._load_user_docs_to_local(user_sheet_name) # Appends to local DB


            st.session_state['data_loaded'] = True
            st.session_state['last_load_time'] = datetime.now()
            print("Data load complete.")

            # Set session state flags after successful load
            st.session_state['data_loaded'] = True
            st.session_state['last_load_time'] = datetime.now()
            print("Data load complete.")

    def _load_user_docs_to_local(self, sheet_name):
        """Loads a specific user document sheet and APPENDS to the local 'documentos' table."""
        ws = self._get_worksheet(sheet_name)
        if not ws:
              print(f"User document sheet '{sheet_name}' not found. Skipping.")
              return True # Not a critical error if a user sheet doesn't exist yet

        print(f"Loading and appending docs from '{sheet_name}'...")
        try:
            data = ws.get_all_records(head=1)
            if not data:
                print(f"User sheet '{sheet_name}' is empty.")
                return True

            df = pd.DataFrame(data)
            
            # Validate/align columns
            missing_cols = [col for col in config.DOCS_COLS if col not in df.columns]
            for col in missing_cols: df[col] = None
            df = df[config.DOCS_COLS]
            df = df.astype(str) # Convert to string for SQLite TEXT

            df['is_synced'] = 1
            
            if 'id' in df.columns:
                    mask_missing_id = df['id'].isin(['', 'None', None, 'nan', 'NA'])
                    num_missing = mask_missing_id.sum()
                    if num_missing > 0:
                        print(f"Generating {num_missing} missing IDs for docs loaded from '{sheet_name}'")
                        df.loc[mask_missing_id, 'id'] = [str(uuid.uuid4()) for _ in range(num_missing)]

              # Append to local SQLite table (agora inclui a coluna is_synced)
              # Precisa garantir que a tabela local tenha a coluna antes! (feito em _create_local_tables)
            df.to_sql("documentos", self.local_conn, if_exists='append', index=False, chunksize=1000)
            print(f"Appended {len(df)} rows from '{sheet_name}' to local 'documentos' (marked as synced).")
            return True
        except Exception as e:
              st.error(f"Error loading user docs from '{sheet_name}': {e}")
              return False


    # --- Local Read Methods (Operating on SQLite Cache) ---

    def buscar_usuario_local(self, username):
        """Fetches a user from the local SQLite cache."""
        return self._execute_local_sql("SELECT * FROM usuarios WHERE username = ?", (username,), fetch_mode="one")

    def listar_clientes_local(self, colaborador_username=None):
         """Lists clients from local cache, optionally filtered by assignment."""
         if colaborador_username:
             # Find clients assigned to this collaborator
             query = """
                 SELECT c.id, c.nome, c.tipo
                 FROM clientes c
                 JOIN colaborador_cliente ca ON c.nome = ca.cliente_nome COLLATE NOCASE -- Join by name, case-insensitive
                 WHERE ca.colaborador_username = ? COLLATE NOCASE
                 ORDER BY c.nome
             """
             return self._execute_local_sql(query, (colaborador_username,))
         else:
             # List all clients (for Admin or general dropdowns)
             return self._execute_local_sql("SELECT id, nome, tipo FROM clientes ORDER BY nome")

    def listar_colaboradores_local(self):
        """Lists all 'Usuario' role users from local cache."""
        return self._execute_local_sql("SELECT username, nome_completo FROM usuarios WHERE role = 'Usuario' ORDER BY nome_completo")

    def get_kpi_data_local(self, colaborador_username=None, cliente_nome=None, periodo_dias=None):
         """Calculates KPIs based on the local 'documentos' table, with more filters."""
         base_query = "SELECT status, COUNT(*) as count FROM documentos WHERE 1=1" # Start with true condition
         params = []

         if colaborador_username:
              base_query += " AND colaborador_username = ? COLLATE NOCASE"
              params.append(colaborador_username)
         if cliente_nome:
              base_query += " AND cliente_nome = ? COLLATE NOCASE"
              params.append(cliente_nome)
         if periodo_dias:
             # SQLite date filtering needs ISO format or Julian day typically
             # Assuming 'data_registro' is stored as TEXT in ISO format
             try:
                cutoff_date = datetime.now() - pd.Timedelta(days=periodo_dias)
                cutoff_iso = cutoff_date.isoformat()
                base_query += " AND data_registro >= ?" # Filter based on registration date
                params.append(cutoff_iso)
             except Exception as e:
                print(f"Warning: Could not apply date filter (days={periodo_dias}): {e}")


         query = f"{base_query} GROUP BY status"
         results = self._execute_local_sql(query, tuple(params) if params else None)

         # Rename KPI keys to match client layout image
         kpi = {'docs_enviados': 0, 'docs_publicados': 0, 'docs_pendentes': 0, 'docs_invalidos': 0}
         if results:
             # Map based on status values in your data
             status_map = {
                  'Enviado': 'docs_enviados',
                  'Validado': 'docs_publicados', # Assuming 'Validado' means 'Publicado'
                  'Pendente': 'docs_pendentes',
                  'Novo': 'docs_pendentes', # Treat 'Novo' as pending for KPI?
                  'Inválido': 'docs_invalidos'
             }
             for row in results:
                  status_key = status_map.get(row['status'])
                  if status_key: kpi[status_key] += row['count']

         return kpi
     
         
    def calcular_pontuacao_colaboradores_local(self):
        """Calculates collaborator scores based on local SQLite data."""
        # This SQL is more complex and joins usuarios and documentos (local tables)
        # Ensure 'status' and column names match your local schema (config.DOCS_COLS)
        query = """
            SELECT
                u.nome_completo,
                COALESCE(SUM(CASE WHEN d.status = 'Validado' THEN 1 ELSE 0 END), 0) as links_validados,
                (SELECT COUNT(*) FROM documentos d2 WHERE d2.colaborador_username = u.username) as total_links_colab,
                COALESCE(SUM(CASE WHEN d.status = 'Validado' THEN 1 ELSE 0 END), 0) * 10 as pontuacao
            FROM usuarios u
            LEFT JOIN documentos d ON u.username = d.colaborador_username AND d.status = 'Validado' -- Join only validated for counting score
            WHERE u.role = 'Usuario'
            GROUP BY u.username, u.nome_completo
            ORDER BY pontuacao DESC, u.nome_completo ASC
        """
        results = self._execute_local_sql(query)
        if not results:
             return pd.DataFrame({'Colaborador': [], 'Pontuação': [], 'Links Validados': [], 'Percentual': []})

        df = pd.DataFrame([dict(row) for row in results])

        # Calculate overall percentage
        total_validados_geral = df['links_validados'].sum()
        df['Percentual'] = (df['links_validados'] / total_validados_geral * 100) if total_validados_geral > 0 else 0.0
        
        df_display = df[['nome_completo', 'pontuacao', 'links_validados', 'Percentual']].rename(columns={
            'nome_completo': 'Colaborador',
            'pontuacao': 'Pontuação',
            'links_validados': 'Links Validados'
        })
        
        return df_display.set_index('Colaborador')
        
    def get_docs_por_periodo_cliente_local(self, cliente_nome, grupo='W'):
        """Gets validated docs count per period for a client from local data."""
        # Assuming 'data_registro' is the relevant date column in 'documentos'
        # and 'cliente_nome' exists in 'documentos'
        
        # Adjust date function based on SQLite version and desired grouping
        # %Y-%W (ISO Week), %Y-%m-%d (Day), %Y-%m (Month)
        format_map = {'W': '%Y-%W', 'D': '%Y-%m-%d', 'M': '%Y-%m'}
        sql_format = format_map.get(grupo, '%Y-%W')

        # Validate 'data_registro' - ensure it can be parsed by strftime
        query = f"""
            SELECT
                strftime('{sql_format}', data_registro) as periodo,
                COUNT(id) as contagem
            FROM documentos
            WHERE cliente_nome = ? AND status = 'Validado' AND data_registro IS NOT NULL
            GROUP BY periodo
            HAVING periodo IS NOT NULL
            ORDER BY MIN(data_registro) ASC -- Order by the actual date, not the formatted string
        """
        results = self._execute_local_sql(query, (cliente_nome,))
        if not results:
            return pd.DataFrame({'periodo': [], 'contagem': [], 'periodo_dt': []})
        
        df = pd.DataFrame([dict(row) for row in results])
        
        # Convert 'periodo' string back to datetime for plotting
        try:
            if grupo == 'W':
                 # Monday as the first day of the week (%w = 0 is Sunday)
                 df['periodo_dt'] = pd.to_datetime(df['periodo'] + '-1', format='%Y-%W-%w', errors='coerce')
            elif grupo == 'M':
                 df['periodo_dt'] = pd.to_datetime(df['periodo'] + '-01', errors='coerce')
            else: # 'D'
                 df['periodo_dt'] = pd.to_datetime(df['periodo'], errors='coerce')
            
            # Drop rows where date conversion failed
            df.dropna(subset=['periodo_dt'], inplace=True)
            df.sort_values('periodo_dt', inplace=True)

        except Exception as e_pd:
             print(f"Error converting period string to datetime: {e_pd}")
             # Return dataframe without 'periodo_dt' if conversion fails broadly
             return df[['periodo', 'contagem']]

        return df[['periodo', 'contagem', 'periodo_dt']]
        
    def get_criterios_atendidos_cliente_local(self, cliente_nome):
        """Gets criteria counts for a client from local data."""
        # Assumes 'dimensao_criterio' holds values like 'Essencial', 'Obrigatório', etc.
        query = """
            SELECT
                dimensao_criterio,
                COUNT(id) as total_docs,
                SUM(CASE WHEN status = 'Validado' THEN 1 ELSE 0 END) as docs_validados
            FROM documentos
            WHERE cliente_nome = ? AND dimensao_criterio IN ('Essencial', 'Obrigatório', 'Recomendado') -- Match case/values
            GROUP BY dimensao_criterio
        """
        results = self._execute_local_sql(query, (cliente_nome,))

        crit_data = {}
        tipos_criterio_config = list(config.CRITERIA_COLORS.keys()) # Get from config
        for crit in tipos_criterio_config: crit_data[crit] = {'total': 0, 'atendidos': 0}

        if results:
            for row in results:
                 tipo = row['dimensao_criterio']
                 if tipo in crit_data:
                     crit_data[tipo]['total'] = row['total_docs'] or 0
                     crit_data[tipo]['atendidos'] = row['docs_validados'] or 0
        return crit_data

    def get_documentos_usuario_local(self, username, synced_status=None):
        """
        Retrieves document entries for a specific user from local SQLite.
        Can filter by sync status (0=unsynced, 1=synced, None=all).
        """
        query = "SELECT * FROM documentos WHERE colaborador_username = ? COLLATE NOCASE"
        params = [username]
        if synced_status is not None and synced_status in [0, 1]:
            query += " AND is_synced = ?"
            params.append(synced_status)
        query += " ORDER BY data_registro DESC, id DESC" # Ordena pelos mais recentes
        return self._execute_local_sql(query, tuple(params))

    def get_unsynced_documents_local(self, username):
        """ Fetches only locally added documents that haven't been synced. """
        return self.get_documentos_usuario_local(username, synced_status=0)

    @st.cache_data(ttl=900) # Cache por 15 minutos para reduzir chamadas API
    def calcular_pontuacao_colaboradores_gsheet(_self):
        """
        Calcula a pontuação, contagem e percentual de links validados dos colaboradores
        lendo DIRETAMENTE das planilhas Google Sheets relevantes.
        AVISO: Esta função pode ser lenta devido a múltiplas chamadas de API.
        """
        print("Calculando pontuação de colaboradores diretamente do Google Sheets (pode ser lento)...")
        df_pontuacao_final = pd.DataFrame({
            'Colaborador': [], 'Pontuação': [], 'Links Validados': [], 'Percentual': []
        }).set_index('Colaborador')

        try:
            # 1. Buscar todos os usuários com role 'Usuario' da planilha principal
            users_ws = _self._get_worksheet(config.SHEET_USERS)
            if not users_ws:
                st.error("Planilha 'usuarios' não encontrada para cálculo de pontuação GSheet.")
                return df_pontuacao_final # Retorna vazio se não encontrar usuários

            all_users_data = users_ws.get_all_records()
            colaboradores_info = [
                {'username': u.get('username'), 'nome_completo': u.get('nome_completo')}
                for u in all_users_data if u.get('role') == 'Usuario' and u.get('username')
            ]

            if not colaboradores_info:
                print("Nenhum usuário com perfil 'Usuario' encontrado na planilha.")
                return df_pontuacao_final # Retorna vazio se não há colaboradores

            # 2. Iterar por cada colaborador e contar seus documentos validados
            validated_counts = {} # Armazena username: count
            total_validated_overall = 0

            print(f"Encontrados {len(colaboradores_info)} colaboradores. Buscando documentos validados...")
            processed_count = 0
            for user_info in colaboradores_info:
                username = user_info['username']
                sheet_name = _self._get_user_sheet_name(username)
                user_validated_count = 0
                try:
                    docs_ws = _self._get_worksheet(sheet_name) # Usa o helper que já trata WorksheetNotFound
                    if docs_ws:
                        # Ler todos os registros da planilha do usuário
                        # get_all_records pode ser pesado para planilhas grandes
                        docs_data = docs_ws.get_all_records()
                        for record in docs_data:
                            # Verificar status (case-insensitive, remove espaços extras)
                            status = str(record.get('status', '')).strip()
                            if status.lower() == 'validado': # Ou qualquer que seja seu termo exato
                                user_validated_count += 1
                        print(f"  - Usuário '{username}': {user_validated_count} documentos validados encontrados em '{sheet_name}'.")
                    else:
                         print(f"  - Usuário '{username}': Planilha '{sheet_name}' não encontrada ou vazia.")

                except Exception as e:
                     # Loga o erro mas continua, tratando como 0 para este usuário
                     print(f"  - Erro ao processar planilha '{sheet_name}' para usuário '{username}': {e}")
                     # Opcional: st.warning para informar o admin sobre a falha parcial?

                validated_counts[username] = user_validated_count
                total_validated_overall += user_validated_count
                processed_count += 1
                # Opcional: Mostrar progresso se houver muitos usuários?
                # print(f"   Processado {processed_count}/{len(colaboradores_info)}...")


            print(f"Total de documentos validados encontrados em todas as planilhas: {total_validated_overall}")

            # 3. Montar o DataFrame final
            result_data = []
            for user_info in colaboradores_info:
                 username = user_info['username']
                 nome_completo = user_info['nome_completo']
                 links_validados = validated_counts.get(username, 0)
                 pontuacao = links_validados * 10
                 percentual = (links_validados / total_validated_overall * 100) if total_validated_overall > 0 else 0.0

                 result_data.append({
                     'Colaborador': nome_completo,
                     'Pontuação': pontuacao,
                     'Links Validados': links_validados,
                     'Percentual': percentual
                 })

            if result_data:
                 df_pontuacao_final = pd.DataFrame(result_data)
                 # Ordenar
                 df_pontuacao_final.sort_values(by=['Pontuação', 'Colaborador'], ascending=[False, True], inplace=True)
                 # Definir índice
                 df_pontuacao_final.set_index('Colaborador', inplace=True)
                 print("DataFrame de pontuação final (GSheet) criado com sucesso.")

        except gspread.exceptions.APIError as api_err:
             st.error(f"Erro de API do Google ao calcular pontuação GSheet: {api_err}")
             print(f"Erro de API GSheet ao calcular pontuação: {api_err}")
        except Exception as e:
             st.error(f"Erro inesperado ao calcular pontuação GSheet: {e}")
             print(f"Erro inesperado ao calcular pontuação GSheet: {e}")
             import traceback
             traceback.print_exc() # Print traceback para debug no console


        return df_pontuacao_final

    # --- Local Write Methods ---

    def add_documento_local(self, doc_data: dict):
        """ Adds a new document entry locally, marked as unsynced (is_synced = 0). """
        if not doc_data.get('id'):
            doc_data['id'] = str(uuid.uuid4())

        # Inclui o campo is_synced
        all_expected_local_cols = config.DOCS_COLS + ['is_synced']

        for col in all_expected_local_cols:
             doc_data.setdefault(col, None)
             if col == 'is_synced': # <<< Garante que novos registros sejam marcados como NÃO sincronizados
                 doc_data[col] = 0
             else:
                 doc_data[col] = str(doc_data[col]) if doc_data[col] is not None else None


        ordered_values = [doc_data.get(col) for col in all_expected_local_cols]
        placeholders = ", ".join(["?"] * len(all_expected_local_cols))
        cols_str = ", ".join([f'"{col}"' for col in all_expected_local_cols])

        query = f"INSERT INTO documentos ({cols_str}) VALUES ({placeholders})"
        rowcount = self._execute_local_sql(query, tuple(ordered_values), fetch_mode=None)

        if rowcount == 1:
            st.session_state['unsaved_changes'] = True
            print(f"Documento local adicionado (unsynced): {doc_data.get('id')}")
            return True
        else:
            st.error("Falha ao adicionar documento localmente.")
            return False


    # --- Write-Back to Google Sheets ---

    def save_user_data_to_sheets(self, username):
        """
        Saves all documents for the specified user FROM the local SQLite cache
        TO their dedicated Google Sheet, overwriting the sheet's content.
        Also updates the last_sync_timestamp for the user.
        """
        user_sheet_name = self._get_user_sheet_name(username)
        print(f"Iniciando salvamento para o usuário '{username}' na planilha '{user_sheet_name}'...")
        with st.spinner(f"Salvando dados para {username} na planilha..."):
            # 1. Get user's data from local SQLite
            query = "SELECT * FROM documentos WHERE colaborador_username = ?"
            user_docs_local = self._execute_local_sql(query, (username,))
            
            # Check if there are rows to insert
            if user_docs_local:
                 # Convert SQLite Row objects to list of lists for gspread
                 # Important: Ensure the order matches EXACTLY the columns in the GSheet
                 df_local = pd.DataFrame([dict(row) for row in user_docs_local])
                 # Select and order columns according to config.DOCS_COLS
                 df_to_save = df_local[config.DOCS_COLS].astype(str) # Convert all to string for Sheets
                 
                 # Prepare list of lists (header + data)
                 data_to_write = [config.DOCS_COLS] + df_to_save.values.tolist()
                 num_rows = len(data_to_write)
                 print(f"Preparado {num_rows - 1} registros para salvar em '{user_sheet_name}'.")
            else:
                 # If user has no docs locally, just write the header to clear the sheet
                 data_to_write = [config.DOCS_COLS]
                 num_rows = 1
                 print(f"Nenhum registro local para '{username}', planilha '{user_sheet_name}' será limpa/terá apenas cabeçalho.")


            # 2. Get or Create the target Google Sheet Worksheet
            try:
                ws = self.spreadsheet.worksheet(user_sheet_name)
                print(f"Planilha '{user_sheet_name}' encontrada.")
            except gspread.exceptions.WorksheetNotFound:
                 print(f"Planilha '{user_sheet_name}' não encontrada. Tentando criar...")
                 try:
                      # Create sheet with appropriate columns based on config.DOCS_COLS
                      # Add rows based on expected data size + buffer? Or start small?
                      ws = self.spreadsheet.add_worksheet(title=user_sheet_name, rows=max(100, num_rows + 10), cols=len(config.DOCS_COLS))
                      ws.update([config.DOCS_COLS]) # Write header immediately
                      print(f"Planilha '{user_sheet_name}' criada.")
                 except Exception as create_e:
                      st.error(f"Falha ao criar planilha '{user_sheet_name}': {create_e}")
                      return False # Cannot proceed without the sheet

            # 3. Clear and Write data to the Google Sheet
            try:
                # ws.clear() # Clear the sheet first
                # ws.update(data_to_write, value_input_option='USER_ENTERED')
                
                # Optimized Write: Clear contents then update in one call if possible
                # Ensure range is large enough or resize if needed.
                # Safer approach: Clear, then update cells. Adjust range size dynamically.
                existing_rows = ws.row_count
                needed_rows = len(data_to_write)
                if needed_rows > existing_rows:
                     ws.add_rows(needed_rows - existing_rows) # Add necessary rows
                
                # Define the target range string
                range_str = f'A1:{gspread.utils.rowcol_to_a1(needed_rows, len(config.DOCS_COLS))}'
                
                # Clear previous content ONLY in the needed range + maybe a bit below
                # Be careful not to clear excessively if sheet is large
                clear_range_end_row = max(needed_rows, 50) # Clear at least 50 rows or needed rows
                clear_range_str = f'A1:{gspread.utils.rowcol_to_a1(clear_range_end_row, len(config.DOCS_COLS))}'
                # Generate list of empty cells for clearing - This might be slow for large ranges
                # empty_cells = [['' for _ in range(len(config.DOCS_COLS))] for _ in range(clear_range_end_row)]
                # ws.update(clear_range_str, empty_cells, value_input_option='USER_ENTERED') # This could be slow
                # A faster clear might be batch clear but requires more setup, ws.clear() might be okay
                ws.clear() # Simpler, but clears formatting too.

                # Write the new data
                ws.update(range_str, data_to_write, value_input_option='USER_ENTERED')

                print(f"Dados salvos com sucesso em '{user_sheet_name}'.")

                # 4. Update last_sync_timestamp for the user
                if not self._update_last_sync_time_gsheet(username):
                    st.warning("Dados salvos na planilha de documentos, mas falha ao atualizar o timestamp de sincronização.")
                    # Data is saved, but admin view might be slightly off.

                st.session_state['unsaved_changes'] = False # Reset flag
                return True
            except Exception as write_e:
                st.error(f"Falha ao salvar dados na planilha '{user_sheet_name}': {write_e}")
                return False

    def save_selected_docs_to_sheets(self, username, list_of_doc_ids):
         """ Appends selected documents (by ID) to the user's Google Sheet and marks them as synced locally. """
         if not list_of_doc_ids:
              st.warning("Nenhum documento selecionado para salvar.")
              return False # Nada a fazer

         user_sheet_name = self._get_user_sheet_name(username)
         print(f"Iniciando salvamento seletivo para '{username}' na planilha '{user_sheet_name}'...")

         # 1. Get full data for selected IDs from local DB
         placeholders = ','.join('?' * len(list_of_doc_ids))
         # Seleciona APENAS as colunas que vão para o GSheet (config.DOCS_COLS)
         cols_to_select_str = ", ".join([f'"{col}"' for col in config.DOCS_COLS])
         query = f"""
             SELECT {cols_to_select_str}
             FROM documentos
             WHERE colaborador_username = ? COLLATE NOCASE AND id IN ({placeholders}) AND is_synced = 0
         """
         params = tuple([username] + list_of_doc_ids)
         docs_to_save = self._execute_local_sql(query, params)

         if not docs_to_save:
              st.error("Não foi possível encontrar os documentos selecionados não sincronizados no cache local.")
              return False

         # 2. Prepare data for gspread append_rows (list of lists, no header, only config columns)
         data_to_append = []
         saved_ids_confirm = [] # Para marcar como synced localmente depois
         for row in docs_to_save:
              row_dict = dict(row)
              # Garante a ordem correta das colunas do config
              ordered_row_values = [str(row_dict.get(col, '')) for col in config.DOCS_COLS]
              data_to_append.append(ordered_row_values)
              saved_ids_confirm.append(row_dict.get('id')) # Guarda o ID

         if not data_to_append:
             st.error("Falha ao preparar dados para envio (formato inesperado).")
             return False

         # 3. Get user's worksheet (Create if not exists? Talvez não deveria criar aqui, só no cadastro)
         ws = self._get_worksheet(user_sheet_name)
         if not ws:
              st.error(f"Planilha do usuário '{user_sheet_name}' não encontrada. Não é possível salvar.")
              return False # Não salva se a planilha destino não existe

         # 4. Append rows to Google Sheet
         try:
              print(f"Anexando {len(data_to_append)} registros na planilha '{user_sheet_name}'...")
              ws.append_rows(data_to_append, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS', table_range='A1') # Anexa no final
              print("Registros anexados com sucesso na planilha.")

              # 5. Mark rows as synced locally
              if saved_ids_confirm:
                    placeholders_update = ','.join('?' * len(saved_ids_confirm))
                    update_query = f"UPDATE documentos SET is_synced = 1 WHERE id IN ({placeholders_update}) AND colaborador_username = ?"
                    update_params = tuple(saved_ids_confirm + [username])
                    rows_updated = self._execute_local_sql(update_query, update_params, fetch_mode=None)
                    print(f"{rows_updated} registros marcados como sincronizados localmente.")
                    if rows_updated != len(saved_ids_confirm):
                         st.warning("A contagem de registros marcados como sincronizados localmente não bate com a contagem enviada.")

                    # 6. Update global sync timestamp
                    self._update_last_sync_time_gsheet(username)

                    # 7. Check if there are still unsaved changes
                    remaining_unsaved = self.get_unsynced_documents_local(username)
                    if not remaining_unsaved:
                         st.session_state['unsaved_changes'] = False # Só muda se TUDO foi salvo
                    else:
                         st.session_state['unsaved_changes'] = True # Mantém True se ainda há pendentes

                    return True # Sucesso geral
              else:
                    st.warning("Nenhum ID confirmado para marcar como sincronizado localmente.")
                    return False # Algo deu errado

         except Exception as append_e:
              st.error(f"Falha ao anexar dados na planilha '{user_sheet_name}': {append_e}")
              # Não marcar como sincronizado localmente se a escrita no GSheet falhou
              return False

    def _update_last_sync_time_gsheet(self, username):
        """Updates the 'last_sync_timestamp' column in the main 'usuarios' sheet."""
        users_ws = self._get_worksheet(config.SHEET_USERS)
        if not users_ws:
            st.error("Planilha 'usuarios' não encontrada para atualizar timestamp.")
            return False
        
        try:
            # Find the row for the user
            cell = users_ws.find(username, in_column=config.USERS_COLS.index('username') + 1) # Find username in the first column (adjust index if needed)
            if not cell:
                 print(f"Usuário '{username}' não encontrado na planilha 'usuarios' para atualizar timestamp.")
                 return False # User not found

            user_row_index = cell.row
            # Find the column index for the timestamp
            try:
                 timestamp_col_index = config.USERS_COLS.index('last_sync_timestamp') + 1
            except ValueError:
                 st.error("Coluna 'last_sync_timestamp' não definida em config.USERS_COLS ou não encontrada na planilha.")
                 return False
                 
            # Update the cell
            now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
            users_ws.update_cell(user_row_index, timestamp_col_index, now_str)
            print(f"Timestamp de sincronização atualizado para '{username}'.")
            # Update local cache as well
            self._execute_local_sql("UPDATE usuarios SET last_sync_timestamp = ? WHERE username = ?", (now_str, username), fetch_mode=None)
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar timestamp para {username} na planilha 'usuarios': {e}")
            return False
            
    # --- Admin specific methods ---
    def get_all_users_local_with_sync(self):
        """Gets all users from local cache including sync time."""
        return self._execute_local_sql("SELECT username, nome_completo, role, last_sync_timestamp FROM usuarios ORDER BY nome_completo")

    def get_user_last_update_time(self, username):
         """ Fetches the sync time specifically for the admin view """
         user_info = self._execute_local_sql("SELECT last_sync_timestamp FROM usuarios WHERE username = ?", (username,), fetch_mode="one")
         return user_info['last_sync_timestamp'] if user_info else "N/A"


    def __del__(self):
        """Close the local SQLite connection when the object is garbage collected."""
        if hasattr(self, 'local_conn') and self.local_conn:
            self.local_conn.close()
            print("Local SQLite connection closed.")
            
    def get_analise_cliente_data_local(self, cliente_nome, colaborador_username=None):
         """ Fetches data needed for the 'Análise por Cliente' donut charts. """
         analise = {'docs_no_drive': config.DOCS_NO_DRIVE_TARGET, 'docs_publicados': 0, 'docs_pendentes': 0, 'criterios_counts': {}}

         base_query = "SELECT status, dimensao_criterio, COUNT(id) as count FROM documentos WHERE cliente_nome = ? COLLATE NOCASE"
         params = [cliente_nome]

         if colaborador_username: # Filter further if it's a Usuario view
             base_query += " AND colaborador_username = ? COLLATE NOCASE"
             params.append(colaborador_username)

         query = f"{base_query} GROUP BY status, dimensao_criterio"
         results = self._execute_local_sql(query, tuple(params))

         if not results:
             analise['docs_pendentes'] = analise['docs_no_drive'] # If no docs found, all are pending
             return analise

         crit_counts = {}
         total_published = 0

         for row in results:
             if row['status'] == 'Validado': # Assuming 'Validado' means 'Publicado'
                 total_published += row['count']

             # Count documents per criteria type (ignoring status here, just count occurrences)
             crit = row['dimensao_criterio']
             if crit and crit in config.CRITERIA_COLORS: # Check if it's a defined criterion
                crit_counts[crit] = crit_counts.get(crit, 0) + row['count']

         analise['docs_publicados'] = total_published
         # Calculate pending based on the target in config
         analise['docs_pendentes'] = max(0, analise['docs_no_drive'] - total_published)
         analise['criterios_counts'] = crit_counts

         return analise
    
    def get_assigned_clients_local(self, colaborador_username):
        """Gets list of client names assigned to a collaborator from local cache."""
        query = "SELECT cliente_nome FROM colaborador_cliente WHERE colaborador_username = ? COLLATE NOCASE ORDER BY cliente_nome"
        results = self._execute_local_sql(query, (colaborador_username,))
        return [row['cliente_nome'] for row in results] if results else []

    def assign_clients_to_collab(self, colaborador_username, client_names_to_assign):
        """Assigns clients to a collaborator, updating local DB and GSheets."""
        if not colaborador_username or not client_names_to_assign:
            st.warning("Nome de colaborador ou lista de clientes está vazia.")
            return False

        print(f"Atribuindo clientes {client_names_to_assign} para {colaborador_username}...")
        assignments_to_add_gsheet = []
        assign_success_count = 0
        assign_fail_count = 0

        with self.local_conn: # Use transaction for local changes
             cursor = self.local_conn.cursor()
             for cliente_nome in client_names_to_assign:
                  try:
                       # Insert locally (IGNORE on conflict prevents duplicates)
                       cursor.execute("""
                           INSERT OR IGNORE INTO colaborador_cliente (colaborador_username, cliente_nome)
                           VALUES (?, ?)
                       """, (colaborador_username, cliente_nome))
                       if cursor.rowcount > 0: # Only add to GSheet batch if inserted locally
                            assignments_to_add_gsheet.append([colaborador_username, cliente_nome])
                            assign_success_count += 1
                       else:
                            print(f"Atribuição local já existe para {colaborador_username} - {cliente_nome}")
                            assign_success_count += 1 # Count as success if already exists

                  except sqlite3.Error as e:
                       print(f"Erro ao inserir atribuição local: {colaborador_username} -> {cliente_nome}. Error: {e}")
                       assign_fail_count += 1

        # Write batch to Google Sheets if there are new assignments
        if assignments_to_add_gsheet:
             ws = self._get_worksheet(config.SHEET_ASSOC)
             if ws:
                  try:
                       ws.append_rows(assignments_to_add_gsheet, value_input_option='USER_ENTERED')
                       print(f"{len(assignments_to_add_gsheet)} novas atribuições adicionadas à planilha '{config.SHEET_ASSOC}'.")
                  except Exception as e:
                       st.error(f"Erro ao salvar atribuições na planilha '{config.SHEET_ASSOC}': {e}")
                       # Consider rolling back local changes? More complex.
                       return False # Indicate partial or full failure
             else:
                  st.error(f"Planilha de atribuições '{config.SHEET_ASSOC}' não encontrada. Atribuições não salvas na nuvem.")
                  return False # Indicate failure

        if assign_fail_count > 0:
            st.error(f"{assign_fail_count} atribuições falharam ao salvar localmente.")
            return False

        st.success(f"{assign_success_count} atribuições salvas/verificadas com sucesso.")
        return True

    def unassign_clients_from_collab(self, colaborador_username, client_names_to_unassign):
        """Removes client assignments for a collaborator (local and GSheets)."""
        if not colaborador_username or not client_names_to_unassign:
             st.warning("Nome de colaborador ou lista de clientes para desatribuir está vazia.")
             return False

        print(f"Removendo atribuições {client_names_to_unassign} de {colaborador_username}...")
        local_delete_count = 0
        with self.local_conn: # Transaction
             cursor = self.local_conn.cursor()
             placeholders = ','.join('?' * len(client_names_to_unassign))
             params = [colaborador_username] + client_names_to_unassign
             # Delete locally (case-insensitive match)
             cursor.execute(f"""
                 DELETE FROM colaborador_cliente
                 WHERE colaborador_username = ? COLLATE NOCASE
                 AND cliente_nome IN ({placeholders}) COLLATE NOCASE
             """, tuple(params))
             local_delete_count = cursor.rowcount

        if local_delete_count == 0:
             st.warning("Nenhuma atribuição encontrada localmente para remover.")
             # Still try to remove from GSheet in case it's out of sync? Or return success?
             # Let's return True assuming intent was met if nothing was found.
             return True


        # Remove from Google Sheets (more complex - need to find and delete rows)
        ws = self._get_worksheet(config.SHEET_ASSOC)
        if ws:
            try:
                all_assoc_records = ws.get_all_records(head=1)
                rows_to_delete_indices = [] # 1-based index

                # Find rows matching the username and client names to be unassigned
                for i, record in enumerate(all_assoc_records):
                     if str(record.get('colaborador_username')).lower() == colaborador_username.lower() \
                     and str(record.get('cliente_nome')).lower() in [name.lower() for name in client_names_to_unassign]:
                          rows_to_delete_indices.append(i + 2) # +1 for header, +1 for 0-based index -> 1-based gspread index

                # Delete rows in reverse order to avoid index shifting issues
                if rows_to_delete_indices:
                     print(f"Deletando {len(rows_to_delete_indices)} linhas da planilha '{config.SHEET_ASSOC}'...")
                     # Sort indices descending
                     rows_to_delete_indices.sort(reverse=True)
                     # Create list of ranges to delete - More robust methods exist via batchUpdate
                     # Simple approach (can be slow):
                     for row_index in rows_to_delete_indices:
                           try:
                                ws.delete_rows(row_index) # Delete one by one (slow for many)
                           except Exception as del_err:
                                # Log error but continue trying others?
                                print(f"Erro ao deletar linha {row_index} da planilha: {del_err}")
                     print("Remoção da planilha concluída (ou tentativas feitas).")

            except Exception as e:
                st.error(f"Erro ao processar remoção da planilha '{config.SHEET_ASSOC}': {e}")
                st.warning("Atribuições removidas localmente, mas pode ter falhado na planilha.")
                return False # Indicate potential GSheet failure
        else:
             st.error(f"Planilha de atribuições '{config.SHEET_ASSOC}' não encontrada. Atribuições não removidas da nuvem.")
             return False # Indicate GSheet failure

        st.success(f"{local_delete_count} atribuições removidas com sucesso.")
        return True


    def add_cliente_local_and_gsheet(self, nome, tipo):
        """ Adds a client to local cache and Google Sheet """
        # 1. Check duplicate locally first (faster)
        if self._execute_local_sql("SELECT id FROM clientes WHERE nome = ? COLLATE NOCASE", (nome,), fetch_mode="one"):
             st.error(f"Cliente '{nome}' já existe localmente.")
             return False

        # 2. Check duplicate in Google Sheet (slower, more definitive)
        ws = self._get_worksheet(config.SHEET_CLIENTS)
        if ws:
            try:
                 cell = ws.find(nome, in_column=2) # Assumes 'nome' is column B (index 2)
                 if cell:
                      st.error(f"Cliente '{nome}' já existe na planilha.")
                      return False
            except Exception as e:
                 st.error(f"Erro ao verificar duplicidade de cliente na planilha: {e}")
                 return False
        else:
             st.error(f"Planilha '{config.SHEET_CLIENTS}' não encontrada. Não é possível adicionar cliente.")
             return False

        # 3. Generate Unique ID (using UUID)
        client_id = str(uuid.uuid4())

        # 4. Add locally
        add_local_success = False
        try:
            rowcount = self._execute_local_sql(
                "INSERT INTO clientes (id, nome, tipo) VALUES (?, ?, ?)",
                (client_id, nome, tipo), fetch_mode=None
            )
            if rowcount == 1:
                 add_local_success = True
            else:
                 st.error("Falha ao adicionar cliente localmente.")
        except sqlite3.Error as e:
            st.error(f"Erro SQLite ao adicionar cliente: {e}")

        if not add_local_success:
             return False # Don't proceed to add to sheet if local failed

        # 5. Add to Google Sheet
        try:
            # Data in order of CLIENTS_COLS: id, nome, tipo
            client_data = [client_id, nome, tipo]
            ws.append_row(client_data, value_input_option='USER_ENTERED')
            st.success(f"Cliente '{nome}' adicionado com sucesso (Local e Planilha).")
            return True
        except Exception as e:
            st.error(f"Cliente adicionado localmente, mas falha ao adicionar na planilha '{config.SHEET_CLIENTS}': {e}")
            # Rollback local add? More complex. For now, just report inconsistency.
            return False

    def _hash_password(self, password):
        """Hashes a password using SHA256."""
        return hashlib.sha256(password.encode('utf-8')).hexdigest()
# --- Authentication Class (Leveraging local reads) ---

class Autenticador:
    def __init__(self, db_manager: HybridDBManager):
        self.gerenciador_bd = db_manager

    def _hash_password(self, password):
        """Hashes a password using SHA256."""
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    def _verificar_senha(self, stored_hashed_password, provided_password):
        return stored_hashed_password == self._hash_password(provided_password)

    def login(self, username, password):
        # Fetch user directly from the LOCAL cache AFTER data is loaded
        # This requires the DB Manager to be instantiated and loaded first.
        # We handle loading in streamlit_app.py after potential login success.
        
        # TEMPORARY read from sheets just for login check before full load
        # This is slightly inefficient but avoids loading ALL data just to fail login
        print(f"Tentativa de login para {username}. Verificando na planilha...")
        users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
        if not users_ws: return False, "Erro: Planilha de usuários não acessível."
        
        success, user_info_or_error = self._check_login_on_sheets(username, password)

        if success:
             user_info = user_info_or_error # Now it's a dict
             print("Login verificado. Procedendo para carga de dados e definição de sessão...")
             # --- Set basic session state BEFORE loading ---
             st.session_state['logged_in'] = True
             st.session_state['username'] = user_info['username']
             st.session_state['role'] = user_info['role']
             st.session_state['nome_completo'] = user_info['nome_completo']
             st.session_state['cliente_nome'] = None # Reset client name

             # --- Specific logic for 'Cliente' role ---
             if user_info['role'] == 'Cliente':
                  # Assume username IS the client name for login association
                  cliente_login_nome = user_info['username']
                  # Store the presumed client name for data loading/filtering
                  st.session_state['cliente_nome'] = cliente_login_nome
                  print(f"Login Cliente detectado. Associado ao cliente: {cliente_login_nome}")
                  # Optional: Verify this client exists in the clientes sheet/cache later
                  # cliente_check = self.gerenciador_bd._execute_local_sql("SELECT id FROM clientes WHERE nome = ? COLLATE NOCASE", (cliente_login_nome,), fetch_mode='one')
                  # if not cliente_check: st.warning(f"Usuário cliente '{cliente_login_nome}' logado, mas não encontrado na lista de clientes.")


             # --- CRITICAL: Load data into local cache AFTER successful login ---
             try:
                  st.session_state.db_manager.load_data_for_session(
                       st.session_state['username'], st.session_state['role']
                  )
                  st.session_state['data_loaded'] = True
                  print("Carga de dados da sessão concluída.")
             except Exception as load_e:
                  st.error("Falha ao carregar dados após o login. Tente novamente.")
                  st.exception(load_e)
                  # Log out if data load fails? Important to prevent inconsistent state.
                  self._clear_session() # Clear session vars set above
                  st.session_state['logged_in'] = False
                  return False, "Erro no carregamento de dados."

             return True, "Login e carregamento de dados bem-sucedidos." # Return simple success message now
        else:
            return False, user_info_or_error # Return error message

    def _clear_session(self):
        # Helper to clear session state related to login
        keys_to_clear = ['logged_in', 'username', 'role', 'nome_completo', 'cliente_nome', 'data_loaded', 'last_load_time', 'unsaved_changes']
        for key in keys_to_clear:
            if key in st.session_state: del st.session_state[key]
        st.rerun() # Re-initialize with defaults


    def logout(self):
        self._clear_session()
        # Close SQLite connection explicitly? Manager's __del__ should handle it.
        print("Usuário deslogado.")
        st.rerun()
        
    def _check_login_on_sheets(self, username, password):
         # Helper to just check credentials against sheets (separated logic)
         print(f"Verificando credenciais de {username} diretamente na planilha 'usuarios'...")
         users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
         if not users_ws: return False, "Erro: Planilha de usuários não acessível."
         try:
              user_data_list = users_ws.get_all_records()
              user_data = next((record for record in user_data_list if str(record.get('username')) == str(username)), None)

              if user_data and isinstance(user_data, dict):
                   stored_hash = user_data.get('hashed_password')
                   if stored_hash and self._verificar_senha(stored_hash, password):
                        return True, dict(user_data) # Return the user data dict on success
                   else: return False, "Senha incorreta."
              else: return False, "Usuário não encontrado."
         except Exception as e:
              st.error(f"Erro ao verificar usuário na planilha: {e}")
              return False, "Erro ao tentar login."

    def add_default_admin_if_needed(self):
         """ Checks SHEETS if admin exists, adds if not """
         users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
         if not users_ws: return

         try:
              data = users_ws.get_all_records()
              if not data: # Sheet is empty or just header
                    print(f"Nenhum usuário encontrado. Adicionando admin padrão '{config.DEFAULT_ADMIN_USER}'...")
                    hashed_pw = self._hash_password(config.DEFAULT_ADMIN_PASS)
                    admin_data = [
                         config.DEFAULT_ADMIN_USER,
                         hashed_pw,
                         "Administrador Padrão",
                         "Admin",
                         None # last_sync_timestamp
                    ]
                    # Ensure admin_data list matches columns in config.USERS_COLS order
                    users_ws.append_row(admin_data[:len(config.USERS_COLS)], value_input_option='USER_ENTERED')
                    print("Admin padrão adicionado.")
              else:
                   # Check if default admin exists
                   found = False
                   for record in data:
                        if record.get('username') == config.DEFAULT_ADMIN_USER:
                             found = True; break
                   if not found:
                       print(f"Admin '{config.DEFAULT_ADMIN_USER}' não encontrado. Adicionando...")
                       hashed_pw = self._hash_password(config.DEFAULT_ADMIN_PASS)
                       admin_data = [
                           config.DEFAULT_ADMIN_USER, hashed_pw, "Administrador Padrão", "Admin", None
                       ]
                       users_ws.append_row(admin_data[:len(config.USERS_COLS)], value_input_option='USER_ENTERED')
                       print("Admin padrão adicionado.")

         except Exception as e:
              print(f"Erro ao verificar/adicionar admin padrão: {e}")