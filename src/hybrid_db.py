# hybrid_db.py
import streamlit as st
import pandas as pd
import sqlite3
import gspread
from google.oauth2.service_account import Credentials # Explicit import
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
            print("Opening main spreadsheet...")
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
                else: # No fetch needed
                     return None # Or raise error? Indicate no fetch expected
            else: # For INSERT, UPDATE, DELETE
                self.local_conn.commit()
                return cursor.rowcount
        except sqlite3.Error as e:
            st.error(f"Local SQLite Error: {e}\nQuery: {query[:100]}...")
            print(f"Local SQLite Error: {e}\nQuery: {query}\nParams: {params}")
            return None # Or raise e


    def _create_local_tables(self):
        """Creates the necessary tables in the local in-memory SQLite DB."""
        print("Creating local SQLite tables...")
        # --- Usuarios Table ---
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
                id TEXT PRIMARY KEY UNIQUE NOT NULL,
                nome TEXT UNIQUE NOT NULL,
                tipo TEXT
            )
        """)

        # --- Associações Table ---
        self._execute_local_sql("""
           CREATE TABLE IF NOT EXISTS colaborador_cliente (
               colaborador_username TEXT NOT NULL,
               cliente_nome TEXT NOT NULL,
               PRIMARY KEY (colaborador_username, cliente_nome)
           )
        """)

        # --- Documentos Table (Merged) - UPDATED with new columns ---
        cols_config = config.DOCS_COLS
        # Add default values for new columns if needed, TEXT allows NULL by default
        cols_sql = ", ".join([f'"{col}" TEXT' for col in cols_config])
        # Ensure id is PRIMARY KEY and handle is_synced
        cols_sql = cols_sql.replace('"id" TEXT', '"id" TEXT PRIMARY KEY')

        create_docs_sql = f"""
            CREATE TABLE IF NOT EXISTS documentos (
                {cols_sql},
                is_synced INTEGER DEFAULT 0 NOT NULL -- 0 = local only/modified, 1 = synced from GSheet
            )
        """
        self._execute_local_sql(create_docs_sql)
        print("Local SQLite tables created (incluindo tabela de documentos atualizada).")

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
            if table_name not in ["documentos", "colaborador_cliente"]:
                 st.warning(f"Skipping load for non-existent sheet: {sheet_name}")
            else:
                 print(f"Sheet '{sheet_name}' not found, skipping load into '{table_name}'.")
            return True

        print(f"Loading data from GSheet '{sheet_name}' to local table '{table_name}' (mode: {if_exists})...")
        try:
            all_values = ws.get_values()
            if len(all_values) < 1:
                print(f"Sheet '{sheet_name}' is empty or has no header.")
                if if_exists == 'replace': self._execute_local_sql(f"DELETE FROM {table_name}")
                return True

            header = all_values[0]
            data = all_values[1:]

            if not data:
                print(f"Sheet '{sheet_name}' has only a header.")
                if if_exists == 'replace': self._execute_local_sql(f"DELETE FROM {table_name}")
                return True

            df = pd.DataFrame(data, columns=header)

            # --- Column Validation/Alignment (Handles new columns) ---
            actual_header = df.columns.tolist()
            # expected_cols is passed in, should be config.DOCS_COLS for documents
            df_selected = pd.DataFrame(columns=expected_cols) # Empty DF with ALL expected cols from config

            cols_to_copy = [col for col in expected_cols if col in actual_header]
            # ... error check ...
            df_selected[cols_to_copy] = df[cols_to_copy] # Copy existing cols

            # Fill missing expected columns (like new validation cols if sheet is old) with None
            missing_cols = [col for col in expected_cols if col not in actual_header]
            for col in missing_cols:
                df_selected[col] = None # <<< CORRECT: Handles loading old sheets

            df = df_selected[expected_cols] # Ensure correct order

            df = df.astype(str) # Convert all to string for SQLite

            # --- Add is_synced column for 'documentos' table load ---
            if table_name == "documentos":
                df['is_synced'] = 1 # Mark as synced when loading from GSheet

            # Insert into SQLite table
            # Use all expected_cols + is_synced for documentos
            # df.to_sql uses the columns from the DataFrame `df` which now includes *all* expected_cols
            df.to_sql(table_name, self.local_conn, if_exists=if_exists, index=False, chunksize=1000)
            print(f"Successfully loaded {len(df)} rows from '{sheet_name}' to '{table_name}'.")
            return True

        except Exception as e:
            st.error(f"Error loading data from sheet '{sheet_name}': {e}")
            print(f"Traceback during load_sheet_to_local_table for {sheet_name}:")
            import traceback
            traceback.print_exc()
            return False


    def _get_user_sheet_name(self, username):
        """Constructs the expected sheet name for a user's documents."""
        return f"{config.USER_DOCS_SHEET_PREFIX}{username}"

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

    def load_data_for_session(self, username, role):
        """Loads all necessary data from Google Sheets into local SQLite for the session."""
        with st.spinner("Carregando dados da planilha... Por favor, aguarde."):
            print(f"Starting data load for user: {username}, role: {role}")

            # 1. Load Central Sheets (Replace mode)
            load_success = self._load_sheet_to_local_table(config.SHEET_USERS, "usuarios", config.USERS_COLS, if_exists='replace')
            if not load_success: st.stop()
            load_success = self._load_sheet_to_local_table(config.SHEET_CLIENTS, "clientes", config.CLIENTS_COLS, if_exists='replace')
            if not load_success: st.stop()
            load_success = self._load_sheet_to_local_table(config.SHEET_ASSOC, "colaborador_cliente", config.ASSOC_COLS, if_exists='replace')
            if not load_success: print(f"Warning: Falha ao carregar a planilha de associações '{config.SHEET_ASSOC}'.")

            # 2. Load Document Sheets
            # Clear local documents table first
            self._execute_local_sql("DELETE FROM documentos")

            user_sheets_to_load = []
            if role == 'Admin':
                print("Admin role: Loading all user document sheets...")
                users_df = pd.read_sql("SELECT username FROM usuarios WHERE role = 'Usuario'", self.local_conn)
                if not users_df.empty:
                    user_sheets_to_load = [self._get_user_sheet_name(uname) for uname in users_df['username']]
            elif role == 'Usuario':
                user_sheets_to_load = [self._get_user_sheet_name(username)]
                print(f"Loading document sheet for user '{username}': {user_sheets_to_load[0]}")
            elif role == 'Cliente':
                # Load ALL user sheets for now, filtering happens locally.
                # TODO: Future optimization needed here.
                st.warning("Carregamento para Clientes ainda busca em todas as planilhas de usuário.")
                users_df = pd.read_sql("SELECT username FROM usuarios WHERE role = 'Usuario'", self.local_conn)
                if not users_df.empty:
                    user_sheets_to_load = [self._get_user_sheet_name(uname) for uname in users_df['username']]

            all_docs_loaded = True
            for sheet_name in user_sheets_to_load:
                 if not self._load_sheet_to_local_table(sheet_name, "documentos", config.DOCS_COLS, if_exists='append'):
                      all_docs_loaded = False # Keep track if any sheet fails

            if not all_docs_loaded:
                st.warning("Falha ao carregar dados de um ou mais usuários. A visão pode estar incompleta.")

            st.session_state['data_loaded'] = True
            st.session_state['last_load_time'] = datetime.now()
            print(f"Data load complete at {st.session_state['last_load_time']}.")


    # --- Local Read Methods ---

    def buscar_usuario_local(self, username):
        """Fetches a user from the local SQLite cache."""
        return self._execute_local_sql("SELECT * FROM usuarios WHERE username = ?", (username,), fetch_mode="one")

    def listar_clientes_local(self, colaborador_username=None):
         """Lists clients from local cache, optionally filtered by assignment."""
         # ... (keep existing implementation) ...
         if colaborador_username:
             query = """
                 SELECT c.id, c.nome, c.tipo
                 FROM clientes c
                 JOIN colaborador_cliente ca ON c.nome = ca.cliente_nome COLLATE NOCASE
                 WHERE ca.colaborador_username = ? COLLATE NOCASE
                 ORDER BY c.nome
             """
             return self._execute_local_sql(query, (colaborador_username,))
         else:
             return self._execute_local_sql("SELECT id, nome, tipo FROM clientes ORDER BY nome")


    def listar_colaboradores_local(self):
        """Lists all 'Usuario' role users from local cache."""
        # ... (keep existing implementation) ...
        return self._execute_local_sql("SELECT username, nome_completo FROM usuarios WHERE role = 'Usuario' ORDER BY nome_completo")


    def get_kpi_data_local(self, colaborador_username=None, cliente_nome=None, periodo_dias=None):
         """Calculates KPIs based on the local 'documentos' table, with more filters."""
         # ... (keep existing implementation, make sure status names match config.VALID_STATUSES) ...
         base_query = "SELECT status, COUNT(*) as count FROM documentos WHERE 1=1"
         params = []

         if colaborador_username:
              base_query += " AND colaborador_username = ? COLLATE NOCASE"
              params.append(colaborador_username)
         if cliente_nome:
              base_query += " AND cliente_nome = ? COLLATE NOCASE"
              params.append(cliente_nome)
         # ... (period filter remains the same) ...
         if periodo_dias:
             try:
                cutoff_date = datetime.now() - pd.Timedelta(days=periodo_dias)
                cutoff_iso = cutoff_date.isoformat()
                base_query += " AND data_registro >= ?"
                params.append(cutoff_iso)
             except Exception as e:
                print(f"Warning: Could not apply date filter (days={periodo_dias}): {e}")


         query = f"{base_query} GROUP BY status"
         results = self._execute_local_sql(query, tuple(params) if params else None)

         # Rename KPI keys to match client layout image (adjust if needed)
         kpi = {'docs_enviados': 0, 'docs_publicados': 0, 'docs_pendentes': 0, 'docs_invalidos': 0}
         if results:
             status_map = {
                  'Enviado': 'docs_enviados',
                  'Validado': 'docs_publicados', # Maps to 'Publicados' KPI
                  'Pendente': 'docs_pendentes',
                  'Novo': 'docs_pendentes', # Include 'Novo' in 'Pendente'? Or have a separate KPI?
                  'Inválido': 'docs_invalidos'
             }
             for row in results:
                  status_key = status_map.get(row['status'])
                  if status_key: kpi[status_key] += row['count']

         return kpi


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
    
    def get_documentos_usuario_local(self, username, synced_status=None):
        """Retrieves document entries for a specific user from local SQLite. """
        # ... (keep existing implementation) ...
        query = "SELECT * FROM documentos WHERE colaborador_username = ? COLLATE NOCASE"
        params = [username]
        if synced_status is not None and synced_status in [0, 1]:
            query += " AND is_synced = ?"
            params.append(synced_status)
        query += " ORDER BY data_registro DESC, id DESC"
        return self._execute_local_sql(query, tuple(params))

    def get_unsynced_documents_local(self, username):
        """ Fetches only locally added documents that haven't been synced. """
        # ... (keep existing implementation) ...
        return self.get_documentos_usuario_local(username, synced_status=0)


    # --- NEW: Method to get ALL documents for Admin ---
    def get_all_documents_local(self, status_filter=None, user_filter=None, client_filter=None):
        """ Fetches all documents from local cache with optional filters. """
        query = "SELECT * FROM documentos WHERE 1=1"
        params = []

        if status_filter and status_filter != "Todos":
            query += " AND status = ?"
            params.append(status_filter)
        if user_filter: # Assumes user_filter is the username
            query += " AND colaborador_username = ? COLLATE NOCASE"
            params.append(user_filter)
        if client_filter and client_filter != "Todos":
             query += " AND cliente_nome = ? COLLATE NOCASE"
             params.append(client_filter)

        query += " ORDER BY data_registro DESC, colaborador_username, cliente_nome" # Example ordering
        results = self._execute_local_sql(query, tuple(params) if params else None)
        return [dict(row) for row in results] if results else []


    @st.cache_data(ttl=300) # Cache por 2 minutos para reduzir chamadas API
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

        # --- UPDATED: Include new columns with default None ---
        all_expected_local_cols = config.DOCS_COLS + ['is_synced']

        for col in all_expected_local_cols:
             # Set defaults for new validation columns if not provided
             if col in ["data_validacao", "validado_por", "observacoes_validacao"] and col not in doc_data:
                  doc_data[col] = None
             # Handle is_synced specifically
             elif col == 'is_synced':
                  doc_data[col] = 0 # Mark as unsynced on local add
             # Ensure other required cols exist (or set default)
             elif col not in doc_data:
                 doc_data[col] = None # Default to None if missing entirely

             # Convert value to string if not None (for SQLite TEXT)
             if doc_data[col] is not None:
                 doc_data[col] = str(doc_data[col])


        ordered_values = [doc_data.get(col) for col in all_expected_local_cols]
        placeholders = ", ".join(["?"] * len(all_expected_local_cols))
        cols_str = ", ".join([f'"{col}"' for col in all_expected_local_cols]) # Quote column names

        query = f"INSERT INTO documentos ({cols_str}) VALUES ({placeholders})"

        try:
            rowcount = self._execute_local_sql(query, tuple(ordered_values), fetch_mode=None)
            if rowcount == 1:
                st.session_state['unsaved_changes'] = True
                print(f"Documento local adicionado (unsynced): {doc_data.get('id')}")
                return True
            else:
                st.error("Falha ao adicionar documento localmente (rowcount != 1).")
                return False
        except Exception as e:
             st.error(f"Erro ao executar inserção local: {e}")
             return False


    # --- Write-Back to Google Sheets ---

    def save_user_data_to_sheets(self, username):
        """
        Saves all UNVALIDATED documents for the user from local SQLite to GSheet,
        OVERWRITING the sheet's content ONLY with non-validated docs from local.
        NOTE: This OVERWRITE approach is DANGEROUS if Admins validate directly on the sheet.
        The new `update_document_status_gsheet_and_local` is preferred for validation changes.
        This save function should primarily be for the USER syncing their NEW/PENDING entries.
        Consider filtering what gets saved here (e.g., only is_synced=0).
        """
        # --- MODIFIED: Only save locally modified/new rows (is_synced = 0) ---
        user_sheet_name = self._get_user_sheet_name(username)
        print(f"Iniciando salvamento de dados LOCAIS (is_synced=0) para '{username}' na planilha '{user_sheet_name}'...")
        with st.spinner(f"Salvando dados locais de {username} na planilha..."):
            # 1. Get ONLY unsynced user's data from local SQLite
            # Select ONLY the columns defined in config.DOCS_COLS (no is_synced)
            cols_to_select_str = ", ".join([f'"{col}"' for col in config.DOCS_COLS])
            query = f"SELECT {cols_to_select_str} FROM documentos WHERE colaborador_username = ? AND is_synced = 0"
            user_docs_local_unsynced = self._execute_local_sql(query, (username,))

            if not user_docs_local_unsynced:
                 st.info("Nenhum dado local novo ou modificado para salvar.")
                 st.session_state['unsaved_changes'] = False # Clear flag if nothing to save
                 return True # Nothing to do is a success case here

            # Convert SQLite Row objects to list of lists for gspread
            df_local = pd.DataFrame([dict(row) for row in user_docs_local_unsynced])
            # Ensure columns are in the correct order as per config.DOCS_COLS
            df_to_save = df_local[config.DOCS_COLS].astype(str)

            # Prepare list of lists (header + data)
            data_to_write = [config.DOCS_COLS] + df_to_save.values.tolist()
            num_rows_to_write = len(data_to_write) # Includes header
            print(f"Preparado {num_rows_to_write - 1} registros locais para salvar/sobrescrever em '{user_sheet_name}'.")

            # 2. Get or Create the target Google Sheet Worksheet
            # ... (keep existing get/create worksheet logic) ...
            try:
                ws = self.spreadsheet.worksheet(user_sheet_name)
                print(f"Planilha '{user_sheet_name}' encontrada.")
            except gspread.exceptions.WorksheetNotFound:
                 print(f"Planilha '{user_sheet_name}' não encontrada. Tentando criar...")
                 try:
                      ws = self.spreadsheet.add_worksheet(title=user_sheet_name, rows=max(100, num_rows_to_write + 10), cols=len(config.DOCS_COLS))
                      ws.update([config.DOCS_COLS]) # Write header immediately
                      print(f"Planilha '{user_sheet_name}' criada.")
                 except Exception as create_e:
                      st.error(f"Falha ao criar planilha '{user_sheet_name}': {create_e}")
                      return False

            # 3. Clear and Write data to the Google Sheet
            # --- WARNING: This overwrites the sheet ---
            try:
                existing_rows = ws.row_count
                needed_rows = len(data_to_write)
                if needed_rows > existing_rows:
                     ws.add_rows(needed_rows - existing_rows)

                range_str = f'A1:{gspread.utils.rowcol_to_a1(needed_rows, len(config.DOCS_COLS))}'
                ws.clear() # <<< Clears everything
                ws.update(range_str, data_to_write, value_input_option='USER_ENTERED')
                print(f"Dados (is_synced=0) salvos com sucesso em '{user_sheet_name}' (planilha sobrescrita).")

                # 4. Mark the saved rows as synced LOCALLY
                saved_ids = df_to_save['id'].tolist()
                if saved_ids:
                    placeholders = ','.join('?' * len(saved_ids))
                    update_query = f"UPDATE documentos SET is_synced = 1 WHERE id IN ({placeholders}) AND colaborador_username = ?"
                    rows_updated = self._execute_local_sql(update_query, tuple(saved_ids + [username]), fetch_mode=None)
                    print(f"{rows_updated} registros marcados como sincronizados localmente.")

                # 5. Update last_sync_timestamp for the user
                if not self._update_last_sync_time_gsheet(username):
                    st.warning("Dados salvos, mas falha ao atualizar timestamp de sincronização.")

                st.session_state['unsaved_changes'] = False # Reset flag
                return True
            except Exception as write_e:
                st.error(f"Falha ao salvar/sobrescrever dados na planilha '{user_sheet_name}': {write_e}")
                return False

    def save_selected_docs_to_sheets(self, username, list_of_doc_ids):
        """ Appends selected unsynced documents (by ID) to the user's Google Sheet and marks them as synced locally. """
        # --- UPDATED: Include new columns ---
        if not list_of_doc_ids:
             st.warning("Nenhum documento selecionado para salvar.")
             return False

        user_sheet_name = self._get_user_sheet_name(username)
        print(f"Iniciando salvamento seletivo (append) para '{username}' na planilha '{user_sheet_name}'...")

        # 1. Get full data for selected IDs from local DB (only unsynced)
        placeholders = ','.join('?' * len(list_of_doc_ids))
        # Select ALL columns defined in config.DOCS_COLS
        cols_to_select_str = ", ".join([f'"{col}"' for col in config.DOCS_COLS]) # <<< CORRECT: Selects all cols
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

        # 2. Prepare data for gspread append_rows
        data_to_append = []
        saved_ids_confirm = []
        for row in docs_to_save:
            row_dict = dict(row)
            # Ensure correct order based on config.DOCS_COLS
            # Includes new cols, fetching their values (likely None) from local DB
            ordered_row_values = [str(row_dict.get(col, '')) for col in config.DOCS_COLS] # <<< CORRECT
            data_to_append.append(ordered_row_values)
            saved_ids_confirm.append(row_dict.get('id'))

        if not data_to_append:
            st.error("Falha ao preparar dados para envio.")
            return False

        # 3. Get user's worksheet
        ws = self._get_worksheet(user_sheet_name)
        if not ws:
             st.error(f"Planilha do usuário '{user_sheet_name}' não encontrada.")
             # Alternative: Try creating it? Risky if user exists but sheet was deleted.
             # For now, fail if sheet doesn't exist.
             return False

        # 4. Append rows to Google Sheet
        try:
             print(f"Anexando {len(data_to_append)} registros na planilha '{user_sheet_name}'...")
             # Find first empty row (crude way, better methods exist)
             # Or just use append_rows which should handle it
             ws.append_rows(data_to_append, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS', table_range='A1')
             print("Registros anexados com sucesso na planilha.")

             # 5. Mark rows as synced locally
             if saved_ids_confirm:
                 placeholders_update = ','.join('?' * len(saved_ids_confirm))
                 update_query = f"UPDATE documentos SET is_synced = 1 WHERE id IN ({placeholders_update}) AND colaborador_username = ?"
                 update_params = tuple(saved_ids_confirm + [username])
                 rows_updated = self._execute_local_sql(update_query, update_params, fetch_mode=None)
                 print(f"{rows_updated} registros marcados como sincronizados localmente.")
                 if rows_updated != len(saved_ids_confirm):
                      st.warning("Contagem de registros marcados localmente não bate com a contagem enviada.")

                 # 6. Update global sync timestamp
                 self._update_last_sync_time_gsheet(username)

                 # 7. Check if there are still unsaved changes
                 remaining_unsaved = self.get_unsynced_documents_local(username)
                 st.session_state['unsaved_changes'] = bool(remaining_unsaved)

                 return True # Success
             else:
                 st.warning("Nenhum ID confirmado para marcar como sincronizado localmente.")
                 return False

        except Exception as append_e:
             st.error(f"Falha ao anexar dados na planilha '{user_sheet_name}': {append_e}")
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

    # --- NEW: Method to Update Document Status (Admin Validation) ---
    def update_document_status_gsheet_and_local(self, doc_id, new_status, admin_username, observacoes=""):
        """
        Updates the status, validation date, and validator for a specific document
        both in the corresponding Google Sheet and the local cache.
        """
        print(f"Attempting to update doc_id '{doc_id}' to status '{new_status}' by '{admin_username}'...")

        # 1. Find the document locally to get the original collaborator's username
        local_doc = self._execute_local_sql("SELECT colaborador_username FROM documentos WHERE id = ?", (doc_id,), fetch_mode="one")
        if not local_doc:
             st.error(f"Documento com ID '{doc_id}' não encontrado localmente.")
             return False

        colaborador_username = local_doc['colaborador_username']
        user_sheet_name = self._get_user_sheet_name(colaborador_username)

        # 2. Get the specific user's worksheet
        ws = self._get_worksheet(user_sheet_name)
        if not ws:
             st.error(f"Planilha '{user_sheet_name}' para o colaborador '{colaborador_username}' não encontrada.")
             return False

        # 3. Find the row in the Google Sheet based on doc_id
        try:
            # Find the ID column index (assuming it's the first column as per config)
            id_col_index = config.DOCS_COLS.index('id') + 1
            cell = ws.find(doc_id, in_column=id_col_index)
            if not cell:
                 st.error(f"Documento com ID '{doc_id}' não encontrado na planilha '{user_sheet_name}'.")
                 # Maybe it was deleted from the sheet? Or ID mismatch?
                 return False
            row_index = cell.row
            print(f"Found doc_id '{doc_id}' in sheet '{user_sheet_name}' at row {row_index}.")

            # 4. Find column indices for fields to update
            status_col = config.DOCS_COLS.index('status') + 1
            val_date_col = config.DOCS_COLS.index('data_validacao') + 1      
            val_by_col = config.DOCS_COLS.index('validado_por') + 1         
            obs_col = config.DOCS_COLS.index('observacoes_validacao') + 1 

            # 5. Prepare update data
            now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
            updates_batch = [
                {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[new_status]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, val_date_col), 'values': [[now_str]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, val_by_col), 'values': [[admin_username]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, obs_col), 'values': [[observacoes]]}
            ]

            # 6. Update Google Sheet using batch update for efficiency
            ws.batch_update(updates_batch, value_input_option='USER_ENTERED')
            print(f"GSheet row {row_index} updated successfully.")

            # 7. Update local SQLite database
            update_local_query = """
                UPDATE documentos
                SET status = ?, data_validacao = ?, validado_por = ?, observacoes_validacao = ?, is_synced = 1
                WHERE id = ?
            """
            # Mark as synced=1 because the change originated from an action meant to sync
            rows_updated = self._execute_local_sql(
                update_local_query,
                (new_status, now_str, admin_username, observacoes, doc_id),
                fetch_mode=None
            )

            if rows_updated == 1:
                print(f"Local document ID '{doc_id}' updated successfully.")
                # No need to set unsaved_changes = True here, as it was synced.
                return True
            else:
                st.error(f"Falha ao atualizar o registro local para o ID '{doc_id}' (linhas afetadas: {rows_updated}). A planilha foi atualizada.")
                # Data inconsistency state!
                return False

        except gspread.exceptions.APIError as api_err:
             st.error(f"Erro de API do Google ao atualizar status para doc ID '{doc_id}': {api_err}")
             return False
        except ValueError as ve: # Handles if a column name isn't found in config.DOCS_COLS
             st.error(f"Erro de configuração: coluna não encontrada em DOCS_COLS. {ve}")
             return False
        except Exception as e:
             st.error(f"Erro inesperado ao atualizar status para doc ID '{doc_id}': {e}")
             import traceback
             traceback.print_exc()
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
    # ... (keep existing __init__, _hash_password, _verificar_senha) ...
    def __init__(self, db_manager: HybridDBManager):
        self.gerenciador_bd = db_manager

    def _hash_password(self, password):
        """Hashes a password using SHA256."""
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    def _verificar_senha(self, stored_hashed_password, provided_password):
        return stored_hashed_password == self._hash_password(provided_password)


    def login(self, username, password):
        # ... (keep existing login logic, it already loads data post-login) ...
        print(f"Attempting login for {username}. Verifying against GSheet...")
        users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
        if not users_ws: return False, "Error: User worksheet not accessible."

        success, user_info_or_error = self._check_login_on_sheets(username, password)

        if success:
             user_info = user_info_or_error # It's a dict
             print("Login verified. Proceeding to data loading and session setup...")
             # --- Set basic session state BEFORE loading ---
             st.session_state['logged_in'] = True
             st.session_state['username'] = user_info['username']
             st.session_state['role'] = user_info['role']
             st.session_state['nome_completo'] = user_info['nome_completo']
             st.session_state['cliente_nome'] = None # Reset client name

             # --- Specific logic for 'Cliente' role ---
             if user_info['role'] == 'Cliente':
                  cliente_login_nome = user_info['username']
                  st.session_state['cliente_nome'] = cliente_login_nome
                  print(f"Client login detected. Associated client: {cliente_login_nome}")

             # --- CRITICAL: Load data into local cache AFTER successful login ---
             try:
                  # Use the manager instance already in session state if available
                  manager_instance = st.session_state.get('db_manager')
                  if not manager_instance:
                       # This shouldn't happen if initialized correctly in streamlit_app.py
                       st.error("Critical Error: DB Manager not found in session state during login.")
                       self._clear_session()
                       return False, "Internal server error during login."

                  manager_instance.load_data_for_session(
                       st.session_state['username'], st.session_state['role']
                  )
                  # load_data_for_session sets data_loaded and last_load_time internally
                  print("Session data load completed.")
             except Exception as load_e:
                  st.error("Failed to load data after login. Please try again.")
                  st.exception(load_e)
                  self._clear_session() # Log out if data load fails
                  return False, "Data loading error."

             return True, "Login and data loading successful."
        else:
            return False, user_info_or_error # Return error message


    def _clear_session(self):
        # ... (keep existing) ...
        keys_to_clear = ['logged_in', 'username', 'role', 'nome_completo', 'cliente_nome', 'data_loaded', 'last_load_time', 'unsaved_changes', 'db_manager'] # Also clear db_manager? Maybe not if we want to reuse the connection object if possible. Let's keep it for now.
        cleared_keys = []
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
                cleared_keys.append(key)
        print(f"Cleared session keys: {cleared_keys}")
        # st.rerun() # Rerun is handled by logout caller usually

    def logout(self):
        # ... (keep existing) ...
        self._clear_session()
        print("User logged out.")
        st.cache_resource.clear() # Clear resource caches like gspread client
        st.cache_data.clear()     # Clear data caches
        st.rerun() # Force rerun to go back to login state

    def _check_login_on_sheets(self, username, password):
        # ... (keep existing) ...
        print(f"Verifying credentials for {username} directly in '{config.SHEET_USERS}' sheet...")
        users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
        if not users_ws: return False, "Error: User worksheet not accessible."
        try:
              # Use get_all_records for easier dict access
              user_data_list = users_ws.get_all_records()
              # Case-insensitive search for username? Safer.
              user_data = next((record for record in user_data_list
                                if str(record.get('username','')).lower() == str(username).lower()), None)

              if user_data and isinstance(user_data, dict):
                   stored_hash = user_data.get('hashed_password')
                   # Handle potentially empty passwords/hashes carefully
                   if stored_hash and self._verificar_senha(stored_hash, password):
                        # Return a clean dict, converting numeric-like strings if necessary? No, keep as is from sheet.
                        return True, dict(user_data)
                   else:
                        return False, "Incorrect password."
              else:
                    return False, "User not found."
        except Exception as e:
              st.error(f"Error verifying user in the sheet: {e}")
              return False, "Error during login attempt."


    def add_default_admin_if_needed(self):
        """ Checks SHEETS if admin exists, adds if not """
        # ... (keep existing implementation) ...
        users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
        if not users_ws:
            print("Warning: Cannot check/add default admin, user sheet not found.")
            return

        try:
            data = users_ws.get_all_records() # Simpler to check records
            admin_exists = any(record.get('username') == config.DEFAULT_ADMIN_USER for record in data)

            if not admin_exists:
                print(f"Admin '{config.DEFAULT_ADMIN_USER}' not found. Adding...")
                hashed_pw = self._hash_password(config.DEFAULT_ADMIN_PASS)
                admin_data = [
                    config.DEFAULT_ADMIN_USER,
                    hashed_pw,
                    "Administrador Padrão",
                    "Admin",
                    None # last_sync_timestamp
                ]
                # Ensure list length matches number of columns expected by USERS_COLS
                users_ws.append_row(admin_data[:len(config.USERS_COLS)], value_input_option='USER_ENTERED')
                print("Default admin added to the sheet.")
            else:
                 print(f"Default admin '{config.DEFAULT_ADMIN_USER}' already exists.")

        except Exception as e:
             print(f"Error checking/adding default admin: {e}")