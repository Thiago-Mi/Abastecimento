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
        # Run migration after tables are ensured and clients might be loaded (or will be soon)
        # This relies on clients being loaded before documents for the migration to work effectively in one pass
        # Or it should be called after initial data load.
        # For simplicity, we'll call it here. It won't do much if docs aren't loaded yet,
        # but it will ensure the column exists. The load_data_for_session will be more effective.
        self._migrate_add_cliente_id_to_documentos_local()


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
               cliente_id TEXT NOT NULL, -- MUDANÇA AQUI para cliente_id
               PRIMARY KEY (colaborador_username, cliente_id),
               FOREIGN KEY (cliente_id) REFERENCES clientes(id) -- Opcional, mas bom para integridade
           )
        """)

        # --- Documentos Table (Merged) - UPDATED with cliente_id ---
        cols_config = config.DOCS_COLS # This now includes 'cliente_id'
        cols_sql_parts = []
        for col in cols_config:
            col_sql = f'"{col}" TEXT'
            if col == "id":
                col_sql += " PRIMARY KEY"
            cols_sql_parts.append(col_sql)
        
        # Add is_synced separately as it's not in DOCS_COLS
        cols_sql_parts.append("is_synced INTEGER DEFAULT 0 NOT NULL")
        cols_sql = ", ".join(cols_sql_parts)

        create_docs_sql = f"CREATE TABLE IF NOT EXISTS documentos ({cols_sql})"
        self._execute_local_sql(create_docs_sql)
        print("Local SQLite tables created (documentos table now includes cliente_id).")

    def _migrate_add_cliente_id_to_documentos_local(self):
        """
        Scans the local 'documentos' table and adds 'cliente_id' by looking up
        'cliente_nome' in the 'clientes' table. Also ensures the column exists.
        """
        print("Starting migration: Add cliente_id to local documentos table...")
        cursor = self.local_conn.cursor()
        try:
            # 1. Ensure 'cliente_id' column exists in 'documentos'
            cursor.execute("PRAGMA table_info(documentos)")
            columns = [info[1] for info in cursor.fetchall()]
            if 'cliente_id' not in columns:
                self._execute_local_sql("ALTER TABLE documentos ADD COLUMN cliente_id TEXT", fetch_mode=None)
                print("Column 'cliente_id' added to local 'documentos' table.")
            else:
                print("Column 'cliente_id' already exists in local 'documentos' table.")

            # 2. Fetch all clients for mapping
            clients_map_rows = self._execute_local_sql("SELECT id, nome FROM clientes")
            if not clients_map_rows:
                print("Migration: No clients found in local 'clientes' table. Cannot map cliente_id yet.")
                return

            clients_map = {row['nome'].lower(): row['id'] for row in clients_map_rows} # Lowercase for case-insensitive matching

            # 3. Fetch documents that need cliente_id updated
            #    (where cliente_id is NULL but cliente_nome is not)
            docs_to_update = self._execute_local_sql(
                "SELECT id, cliente_nome FROM documentos WHERE cliente_id IS NULL AND cliente_nome IS NOT NULL"
            )

            if not docs_to_update:
                print("Migration: No documents found needing cliente_id update (or all already have it).")
                return

            print(f"Migration: Found {len(docs_to_update)} documents to potentially update with cliente_id.")
            updated_count = 0
            for doc_row in docs_to_update:
                doc_id = doc_row['id']
                cliente_nome = doc_row['cliente_nome']
                if cliente_nome:
                    cliente_id_found = clients_map.get(cliente_nome.lower())
                    if cliente_id_found:
                        self._execute_local_sql(
                            "UPDATE documentos SET cliente_id = ? WHERE id = ?",
                            (cliente_id_found, doc_id), fetch_mode=None
                        )
                        updated_count += 1
                    else:
                        print(f"Migration Warning: Cliente ID not found for cliente_nome '{cliente_nome}' (doc_id: {doc_id}).")
            
            if updated_count > 0:
                self.local_conn.commit()
                print(f"Migration: Successfully updated cliente_id for {updated_count} documents.")
            else:
                print("Migration: No documents were updated with cliente_id in this pass.")

        except sqlite3.Error as e:
            st.error(f"Migration Error (add_cliente_id): {e}")
            print(f"Migration Error (add_cliente_id): {e}")
        except Exception as ex: # Catch other potential errors
            st.error(f"General Migration Error (add_cliente_id): {ex}")
            print(f"General Migration Error (add_cliente_id): {ex}")


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
            if table_name not in ["documentos", "colaborador_cliente"]: # Don't warn for these if they don't exist
                 st.warning(f"Skipping load for non-existent sheet: {sheet_name}")
            else:
                 print(f"Sheet '{sheet_name}' not found, skipping load into '{table_name}'.")
            return True

        print(f"Loading data from GSheet '{sheet_name}' to local table '{table_name}' (mode: {if_exists})...")
        try:
            all_values = ws.get_values() # Get all values, including headers
            if len(all_values) < 1: # Check if sheet is completely empty
                print(f"Sheet '{sheet_name}' is empty.")
                if if_exists == 'replace' and table_name != "documentos": # Don't mass delete documents if one user sheet is empty
                    self._execute_local_sql(f"DELETE FROM {table_name}")
                return True

            header = all_values[0]
            data = all_values[1:]

            if not data: # Check if sheet has only header
                print(f"Sheet '{sheet_name}' has only a header.")
                if if_exists == 'replace' and table_name != "documentos":
                     self._execute_local_sql(f"DELETE FROM {table_name}")
                return True

            df = pd.DataFrame(data, columns=header)

            # --- Column Validation/Alignment ---
            df_selected = pd.DataFrame(columns=expected_cols) # Empty DF with ALL expected cols from config

            # Copy data for columns that exist in both GSheet and expected_cols
            cols_to_copy = [col for col in expected_cols if col in df.columns]
            if not cols_to_copy and expected_cols: # If no common columns but we expect some
                print(f"Warning: No common columns between GSheet '{sheet_name}' header and expected columns for '{table_name}'.")
            df_selected[cols_to_copy] = df[cols_to_copy]

            # Fill missing expected columns with None (e.g. 'cliente_id' if GSheet is old)
            missing_config_cols = [col for col in expected_cols if col not in df.columns]
            for col in missing_config_cols:
                df_selected[col] = None

            # Ensure correct order and only expected columns
            df = df_selected[expected_cols].astype(str) # Convert all to string for SQLite

            # --- Special handling for 'documentos' table ---
            if table_name == "documentos":
                df['is_synced'] = 1 # Mark as synced when loading from GSheet

                # Attempt to fill 'cliente_id' if it's missing (e.g., from older GSheets)
                # This requires 'clientes' table to be loaded first.
                if 'cliente_id' in df.columns and 'cliente_nome' in df.columns:
                    # Get a map of cliente_nome to cliente_id from the local 'clientes' table
                    clients_map_rows = self._execute_local_sql("SELECT id, nome FROM clientes")
                    clients_map = {row['nome'].lower(): row['id'] for row in clients_map_rows} if clients_map_rows else {}
                    
                    def get_cliente_id(row):
                        if pd.isna(row['cliente_id']) or str(row['cliente_id']).strip() == '' or str(row['cliente_id']).lower() == 'none':
                            return clients_map.get(str(row['cliente_nome']).lower())
                        return row['cliente_id']

                    if not clients_map:
                        print(f"Warning: Clientes map is empty. Cannot populate 'cliente_id' for docs from '{sheet_name}' at this stage.")
                    else:
                        df['cliente_id'] = df.apply(get_cliente_id, axis=1)
                        num_filled = df['cliente_id'].notna().sum() - df_selected['cliente_id'].notna().sum()
                        if num_filled > 0:
                            print(f"Filled {num_filled} missing 'cliente_id' values for docs from '{sheet_name}' using local clientes map.")

                # Generate UUIDs for 'id' if missing
                if 'id' in df.columns:
                    mask_missing_id = df['id'].isin(['', 'None', None, 'nan', 'NA', 'NoneType'])
                    num_missing_ids = mask_missing_id.sum()
                    if num_missing_ids > 0:
                        print(f"Generating {num_missing_ids} missing UUIDs for 'id' column in docs from '{sheet_name}'.")
                        df.loc[mask_missing_id, 'id'] = [str(uuid.uuid4()) for _ in range(num_missing_ids)]
            
            # Insert into SQLite table
            df.to_sql(table_name, self.local_conn, if_exists=if_exists, index=False, chunksize=1000)
            print(f"Successfully loaded {len(df)} rows from '{sheet_name}' to '{table_name}'.")
            return True

        except Exception as e:
            st.error(f"Error loading data from sheet '{sheet_name}' to table '{table_name}': {e}")
            print(f"Traceback during load_sheet_to_local_table for {sheet_name}:")
            import traceback
            traceback.print_exc()
            return False

    def _get_user_sheet_name(self, username):
        """Constructs the expected sheet name for a user's documents."""
        return f"{config.USER_DOCS_SHEET_PREFIX}{username}"

    # _load_user_docs_to_local is now handled by _load_sheet_to_local_table with if_exists='append' for documents

    def load_data_for_session(self, username, role):
        """Loads all necessary data from Google Sheets into local SQLite for the session."""
        with st.spinner("Carregando dados da planilha... Por favor, aguarde."):
            print(f"Starting data load for user: {username}, role: {role}")

            # 1. Load Central Sheets (Replace mode)
            load_success = self._load_sheet_to_local_table(config.SHEET_USERS, "usuarios", config.USERS_COLS, if_exists='replace')
            if not load_success: st.stop()
            load_success = self._load_sheet_to_local_table(config.SHEET_CLIENTS, "clientes", config.CLIENTS_COLS, if_exists='replace')
            if not load_success: st.stop() # Clients are crucial for cliente_id mapping
            load_success = self._load_sheet_to_local_table(config.SHEET_ASSOC, "colaborador_cliente", config.ASSOC_COLS, if_exists='replace')
            if not load_success: print(f"Warning: Falha ao carregar a planilha de associações '{config.SHEET_ASSOC}'.")

            # --- Run migration for cliente_id in documentos AFTER clientes table is loaded ---
            # This ensures the clients_map in migration has data
            self._migrate_add_cliente_id_to_documentos_local()


            # 2. Load Document Sheets (Append mode into 'documentos' table)
            # Clear local documents table first to avoid duplicates from previous sessions/users if append is used.
            self._execute_local_sql("DELETE FROM documentos")
            print("Cleared existing local 'documentos' table before loading user sheets.")

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
                # For 'Cliente', load all user sheets. Filtering by 'cliente_nome' (or 'cliente_id')
                # will happen in the UI or data retrieval methods.
                print("Cliente role: Loading all user document sheets for potential visibility...")
                users_df = pd.read_sql("SELECT username FROM usuarios WHERE role = 'Usuario'", self.local_conn)
                if not users_df.empty:
                    user_sheets_to_load = [self._get_user_sheet_name(uname) for uname in users_df['username']]

            all_docs_loaded_successfully = True
            for sheet_name in user_sheets_to_load:
                 # Use 'append' because we cleared 'documentos' once, and now aggregate all user sheets.
                 if not self._load_sheet_to_local_table(sheet_name, "documentos", config.DOCS_COLS, if_exists='append'):
                      all_docs_loaded_successfully = False # Keep track if any sheet fails

            if not all_docs_loaded_successfully:
                st.warning("Falha ao carregar dados de documentos de um ou mais usuários. A visão pode estar incompleta.")

            st.session_state['data_loaded'] = True
            st.session_state['last_load_time'] = datetime.now()
            print(f"Data load complete at {st.session_state['last_load_time']}.")


    # --- Local Read Methods ---

    def buscar_usuario_local(self, username):
        """Fetches a user from the local SQLite cache."""
        return self._execute_local_sql("SELECT * FROM usuarios WHERE username = ?", (username,), fetch_mode="one")

    def listar_clientes_local(self, colaborador_username=None, tipos_filter=None):
         """
         Lists clients from local cache.
         Optionally filtered by assignment to a collaborator and/or by client types.
         """
         query_parts = ["SELECT DISTINCT c.id, c.nome, c.tipo FROM clientes c"] # Use DISTINCT in case of multiple assignments
         params = []

         if colaborador_username:
             query_parts.append("JOIN colaborador_cliente ca ON c.id = ca.cliente_id COLLATE NOCASE") # Using nome for join as in original
             # Consider changing colaborador_cliente to use cliente_id in the future
             query_parts.append("WHERE ca.colaborador_username = ? COLLATE NOCASE")
             params.append(colaborador_username)
         
         if tipos_filter and "Todos" not in tipos_filter and tipos_filter != "Todos": # Handle single string "Todos" or list
            if isinstance(tipos_filter, str): # Single type selected
                tipos_filter = [tipos_filter]
            if isinstance(tipos_filter, list) and tipos_filter: # List of types
                placeholders = ','.join('?' * len(tipos_filter))
                if "WHERE" not in " ".join(query_parts):
                    query_parts.append(f"WHERE c.tipo IN ({placeholders})")
                else:
                    query_parts.append(f"AND c.tipo IN ({placeholders})")
                params.extend(tipos_filter)

         query_parts.append("ORDER BY c.nome")
         query = " ".join(query_parts)
         return self._execute_local_sql(query, tuple(params))


    def listar_colaboradores_local(self):
        """Lists all 'Usuario' role users from local cache."""
        return self._execute_local_sql("SELECT username, nome_completo FROM usuarios WHERE role = 'Usuario' ORDER BY nome_completo")


    def get_kpi_data_local(self, colaborador_username=None, cliente_id=None, periodo_dias=None, tipos_cliente_filter=None):
         """Calculates KPIs based on the local 'documentos' table, with more filters."""
         base_query = """
            SELECT d.status, COUNT(d.id) as count 
            FROM documentos d
         """
         params = []
         conditions = []

         if tipos_cliente_filter and "Todos" not in tipos_cliente_filter and tipos_cliente_filter:
             base_query += " JOIN clientes c ON d.cliente_id = c.id "
             if isinstance(tipos_cliente_filter, str): # Single type
                 tipos_cliente_filter = [tipos_cliente_filter]
             placeholders = ','.join('?'*len(tipos_cliente_filter))
             conditions.append(f"c.tipo IN ({placeholders})")
             params.extend(tipos_cliente_filter)

         if colaborador_username:
              conditions.append("d.colaborador_username = ? COLLATE NOCASE")
              params.append(colaborador_username)
         if cliente_id: # Assuming cliente_id is passed now
              conditions.append("d.cliente_id = ?")
              params.append(cliente_id)
         
         if periodo_dias:
             try:
                cutoff_date = datetime.now() - pd.Timedelta(days=periodo_dias) 
                cutoff_iso = cutoff_date.isoformat()
                conditions.append("d.data_registro >= ?")
                params.append(cutoff_iso)
             except Exception as e:
                print(f"Warning: Could not apply date filter (days={periodo_dias}): {e}")
        
         if conditions:
             base_query += " WHERE " + " AND ".join(conditions)

         query = f"{base_query} GROUP BY d.status"
         results = self._execute_local_sql(query, tuple(params) if params else None)

         kpi = {'docs_enviados': 0, 'docs_validados': 0, 'docs_invalidos': 0}
         
         if results:
             status_map = {
                  'Cadastrado': 'docs_enviados',
                  'Validado': 'docs_validados',
                  'Inválido': 'docs_invalidos'
             }
             for row in results:
                  status_from_db = row['status']
                  count = row['count']
                  if status_from_db in status_map:
                       kpi_key = status_map[status_from_db]
                       kpi[kpi_key] += count
         return kpi


    def get_criterios_atendidos_cliente_local(self, cliente_id): # Changed to cliente_id
        """Gets criteria counts for a client (by ID) from local data."""
        query = """
            SELECT
                d.dimensao_criterio,
                COUNT(d.id) as total_docs,
                SUM(CASE WHEN d.status = 'Validado' THEN 1 ELSE 0 END) as docs_validados
            FROM documentos d
            WHERE d.cliente_id = ? AND d.dimensao_criterio IN ('Essencial', 'Obrigatório', 'Recomendado')
            GROUP BY d.dimensao_criterio
        """
        results = self._execute_local_sql(query, (cliente_id,))

        crit_data = {}
        tipos_criterio_config = list(config.CRITERIA_COLORS.keys())
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
        query = """
            SELECT
                u.nome_completo,
                COALESCE(SUM(CASE WHEN d.status = 'Validado' THEN 1 ELSE 0 END), 0) as links_validados,
                (SELECT COUNT(*) FROM documentos d2 WHERE d2.colaborador_username = u.username) as total_links_colab,
                COALESCE(SUM(CASE WHEN d.status = 'Validado' THEN 1 ELSE 0 END), 0) * 10 as pontuacao 
            FROM usuarios u
            LEFT JOIN documentos d ON u.username = d.colaborador_username 
            WHERE u.role = 'Usuario'
            GROUP BY u.username, u.nome_completo
            ORDER BY pontuacao DESC, u.nome_completo ASC
        """ # Score based on all docs, validated or not, then sum validated for links_validados
        results = self._execute_local_sql(query)
        if not results:
             return pd.DataFrame({'Colaborador': [], 'Pontuação': [], 'Links Validados': [], 'Percentual': []})

        df = pd.DataFrame([dict(row) for row in results])
        df['links_validados'] = df['links_validados'].astype(int) # Ensure it's int
        df['pontuacao'] = df['links_validados'] # Pontuação is just count of validated links

        total_validados_geral = df['links_validados'].sum()
        df['Percentual'] = (df['links_validados'] / total_validados_geral * 100) if total_validados_geral > 0 else 0.0
        
        df_display = df[['nome_completo', 'pontuacao', 'links_validados', 'Percentual']].rename(columns={
            'nome_completo': 'Colaborador',
            'pontuacao': 'Pontuação',
            'links_validados': 'Links Validados'
        })
        
        return df_display.set_index('Colaborador')
    
    def get_docs_por_periodo_cliente_local(self, cliente_id, grupo='W'): # Changed to cliente_id
        """Gets validated docs count per period for a client (by ID) from local data."""
        format_map = {'W': '%Y-%W', 'D': '%Y-%m-%d', 'M': '%Y-%m'}
        sql_format = format_map.get(grupo, '%Y-%W')

        query = f"""
            SELECT
                strftime('{sql_format}', data_registro) as periodo,
                COUNT(id) as contagem
            FROM documentos
            WHERE cliente_id = ? AND status = 'Validado' AND data_registro IS NOT NULL AND data_registro != ''
            GROUP BY periodo
            HAVING periodo IS NOT NULL
            ORDER BY MIN(data_registro) ASC 
        """
        results = self._execute_local_sql(query, (cliente_id,))
        if not results:
            return pd.DataFrame({'periodo': [], 'contagem': [], 'periodo_dt': []})
        
        df = pd.DataFrame([dict(row) for row in results])
        
        try:
            if grupo == 'W':
                 df['periodo_dt'] = pd.to_datetime(df['periodo'] + '-1', format='%Y-%W-%w', errors='coerce')
            elif grupo == 'M':
                 df['periodo_dt'] = pd.to_datetime(df['periodo'] + '-01', format='%Y-%m-%d', errors='coerce') # Explicit day
            else: # 'D'
                 df['periodo_dt'] = pd.to_datetime(df['periodo'], format='%Y-%m-%d', errors='coerce')
            
            df.dropna(subset=['periodo_dt'], inplace=True)
            df.sort_values('periodo_dt', inplace=True)

        except Exception as e_pd:
             print(f"Error converting period string to datetime for cliente_id {cliente_id}: {e_pd}. Data: {df['periodo'].unique()}")
             return df[['periodo', 'contagem']]

        return df[['periodo', 'contagem', 'periodo_dt']]
    
    def get_documentos_usuario_local(self, username, synced_status=None, tipos_cliente_filter=None):
        """Retrieves document entries for a specific user from local SQLite, with optional client type filter."""
        query_parts = ["SELECT d.* FROM documentos d"]
        params = []
        conditions = ["d.colaborador_username = ? COLLATE NOCASE"]
        params.append(username)

        if synced_status is not None and synced_status in [0, 1]:
            conditions.append("d.is_synced = ?")
            params.append(synced_status)

        if tipos_cliente_filter and "Todos" not in tipos_cliente_filter and tipos_cliente_filter:
            query_parts.append("JOIN clientes c ON d.cliente_id = c.id")
            if isinstance(tipos_cliente_filter, str):
                tipos_cliente_filter = [tipos_cliente_filter]
            placeholders = ','.join('?' * len(tipos_cliente_filter))
            conditions.append(f"c.tipo IN ({placeholders})")
            params.extend(tipos_cliente_filter)
        
        if conditions:
            query_parts.append("WHERE " + " AND ".join(conditions))
        
        query_parts.append("ORDER BY d.data_registro DESC, d.id DESC")
        query = " ".join(query_parts)
        return self._execute_local_sql(query, tuple(params))


    def get_unsynced_documents_local(self, username):
        """ Fetches only locally added documents that haven't been synced. """
        return self.get_documentos_usuario_local(username, synced_status=0)


    def get_all_documents_local(self, status_filter=None, user_filter=None, cliente_id_filter=None, tipos_cliente_filter=None):
        """ Fetches all documents from local cache with optional filters, including client type. """
        query_parts = ["SELECT d.*, c.nome as nome_cliente_join, c.tipo as tipo_cliente FROM documentos d LEFT JOIN clientes c ON d.cliente_id = c.id"] # Left join to still get docs if client is somehow missing
        params = []
        conditions = ["1=1"] # Start with a tautology

        if status_filter and status_filter != "Todos":
            conditions.append("d.status = ?")
            params.append(status_filter)
        if user_filter: 
            conditions.append("d.colaborador_username = ? COLLATE NOCASE")
            params.append(user_filter)
        if cliente_id_filter and cliente_id_filter != "Todos": # Assuming "Todos" is a special value for no filter
             conditions.append("d.cliente_id = ?")
             params.append(cliente_id_filter)
        
        if tipos_cliente_filter and "Todos" not in tipos_cliente_filter and tipos_cliente_filter:
            # No need to join again if already joined, but ensure 'c.tipo' is used
            if isinstance(tipos_cliente_filter, str):
                tipos_cliente_filter = [tipos_cliente_filter]
            placeholders = ','.join('?' * len(tipos_cliente_filter))
            conditions.append(f"c.tipo IN ({placeholders})")
            params.extend(tipos_cliente_filter)

        if len(conditions) > 1: # More than just "1=1"
            query_parts.append("WHERE " + " AND ".join(conditions))

        query_parts.append("ORDER BY d.data_registro DESC, d.colaborador_username, d.cliente_nome")
        query = " ".join(query_parts)
        
        results = self._execute_local_sql(query, tuple(params) if params else None)
        return [dict(row) for row in results] if results else []


    @st.cache_data(ttl=300) 
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
            users_ws = _self._get_worksheet(config.SHEET_USERS)
            if not users_ws:
                st.error("Planilha 'usuarios' não encontrada para cálculo de pontuação GSheet.")
                return df_pontuacao_final 

            all_users_data = users_ws.get_all_records()
            colaboradores_info = [
                {'username': u.get('username'), 'nome_completo': u.get('nome_completo')}
                for u in all_users_data if u.get('role') == 'Usuario' and u.get('username')
            ]

            if not colaboradores_info:
                print("Nenhum usuário com perfil 'Usuario' encontrado na planilha.")
                return df_pontuacao_final

            validated_counts = {} 
            total_validated_overall = 0

            print(f"Encontrados {len(colaboradores_info)} colaboradores. Buscando documentos validados...")
            for user_info in colaboradores_info:
                username = user_info['username']
                sheet_name = _self._get_user_sheet_name(username)
                user_validated_count = 0
                try:
                    docs_ws = _self._get_worksheet(sheet_name) 
                    if docs_ws:
                        docs_data = docs_ws.get_all_records() # Might be slow for large sheets
                        for record in docs_data:
                            status = str(record.get('status', '')).strip().lower()
                            if status == 'validado': 
                                user_validated_count += 1
                    # else: # Sheet not found for user, count remains 0
                        # print(f"  - Usuário '{username}': Planilha '{sheet_name}' não encontrada ou vazia.")
                except Exception as e:
                     print(f"  - Erro ao processar planilha '{sheet_name}' para usuário '{username}': {e}")
                validated_counts[username] = user_validated_count
                total_validated_overall += user_validated_count
            
            result_data = []
            for user_info in colaboradores_info:
                 username = user_info['username']
                 nome_completo = user_info['nome_completo']
                 links_validados = validated_counts.get(username, 0)
                 pontuacao = links_validados # Score is number of validated links
                 percentual = (links_validados / total_validated_overall * 100) if total_validated_overall > 0 else 0.0
                 result_data.append({
                     'Colaborador': nome_completo, 'Pontuação': pontuacao,
                     'Links Validados': links_validados, 'Percentual': percentual
                 })

            if result_data:
                 df_pontuacao_final = pd.DataFrame(result_data)
                 df_pontuacao_final.sort_values(by=['Pontuação', 'Colaborador'], ascending=[False, True], inplace=True)
                 df_pontuacao_final.set_index('Colaborador', inplace=True)
        except gspread.exceptions.APIError as api_err:
             st.error(f"Erro de API do Google ao calcular pontuação GSheet: {api_err}")
        except Exception as e:
             st.error(f"Erro inesperado ao calcular pontuação GSheet: {e}")
             import traceback; traceback.print_exc()
        return df_pontuacao_final

    # --- Local Write Methods ---

    def add_documento_local(self, doc_data: dict):
        """ Adds a new document entry locally, marked as unsynced (is_synced = 0). """
        if not doc_data.get('id'):
            doc_data['id'] = str(uuid.uuid4())

        # Fetch cliente_id based on cliente_nome if not already provided
        if not doc_data.get('cliente_id') and doc_data.get('cliente_nome'):
            cliente_info = self._execute_local_sql(
                "SELECT id FROM clientes WHERE nome = ? COLLATE NOCASE",
                (doc_data['cliente_nome'],), fetch_mode="one"
            )
            if cliente_info:
                doc_data['cliente_id'] = cliente_info['id']
            else:
                st.error(f"Não foi possível encontrar o ID para o cliente '{doc_data['cliente_nome']}'. Documento não será salvo com ID de cliente.")
                # Optionally, prevent saving or save with NULL cliente_id depending on requirements
                # doc_data['cliente_id'] = None # Or return False

        all_expected_local_cols = config.DOCS_COLS + ['is_synced']
        final_doc_data = {}

        for col in all_expected_local_cols:
            if col == 'is_synced':
                final_doc_data[col] = 0 # Mark as unsynced
            elif col in doc_data:
                final_doc_data[col] = str(doc_data[col]) if doc_data[col] is not None else None
            else: # Default for missing columns (like validation cols if not provided)
                final_doc_data[col] = None
        
        ordered_values = [final_doc_data.get(col) for col in all_expected_local_cols]
        placeholders = ", ".join(["?"] * len(all_expected_local_cols))
        cols_str = ", ".join([f'"{col}"' for col in all_expected_local_cols])

        query = f"INSERT INTO documentos ({cols_str}) VALUES ({placeholders})"

        try:
            rowcount = self._execute_local_sql(query, tuple(ordered_values), fetch_mode=None)
            if rowcount == 1:
                st.session_state['unsaved_changes'] = True
                print(f"Documento local adicionado (unsynced): {final_doc_data.get('id')}")
                return True
            else:
                st.error("Falha ao adicionar documento localmente (rowcount != 1).")
                return False
        except Exception as e:
             st.error(f"Erro ao executar inserção local: {e}")
             return False


    def save_selected_docs_to_sheets(self, username, list_of_doc_ids):
        """ Appends selected unsynced documents (by ID) to the user's Google Sheet and marks them as synced locally. """
        if not list_of_doc_ids:
             st.warning("Nenhum documento selecionado para salvar.")
             return False

        user_sheet_name = self._get_user_sheet_name(username)
        print(f"Iniciando salvamento seletivo (append) para '{username}' na planilha '{user_sheet_name}'...")

        placeholders = ','.join('?' * len(list_of_doc_ids))
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

        data_to_append = []
        saved_ids_confirm = []
        for row_sqlite in docs_to_save:
            row_dict = dict(row_sqlite)
            # Ensure cliente_id is present, try to fetch if missing (should be rare here if add_documento_local worked)
            if not row_dict.get('cliente_id') and row_dict.get('cliente_nome'):
                 client_obj = self._execute_local_sql("SELECT id FROM clientes WHERE nome = ? COLLATE NOCASE", (row_dict['cliente_nome'],), fetch_mode="one")
                 if client_obj: row_dict['cliente_id'] = client_obj['id']

            ordered_row_values = [str(row_dict.get(col, '')) for col in config.DOCS_COLS]
            data_to_append.append(ordered_row_values)
            saved_ids_confirm.append(row_dict.get('id'))

        if not data_to_append:
            st.error("Falha ao preparar dados para envio (nenhum dado para anexar).")
            return False

        ws = self._get_worksheet(user_sheet_name)
        if not ws:
            # Try to create the sheet if it doesn't exist
            print(f"Planilha '{user_sheet_name}' não encontrada. Tentando criar...")
            try:
                ws = self.spreadsheet.add_worksheet(title=user_sheet_name, rows=max(100, len(data_to_append) + 20), cols=len(config.DOCS_COLS))
                ws.update([config.DOCS_COLS], value_input_option='USER_ENTERED') # Write header
                print(f"Planilha '{user_sheet_name}' criada com sucesso.")
            except Exception as create_e:
                st.error(f"Falha ao criar planilha '{user_sheet_name}': {create_e}")
                return False
        try:
             print(f"Anexando {len(data_to_append)} registros na planilha '{user_sheet_name}'...")
             ws.append_rows(data_to_append, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS', table_range='A1')
             print("Registros anexados com sucesso na planilha.")

             if saved_ids_confirm:
                 placeholders_update = ','.join('?' * len(saved_ids_confirm))
                 update_query = f"UPDATE documentos SET is_synced = 1 WHERE id IN ({placeholders_update}) AND colaborador_username = ?"
                 update_params = tuple(saved_ids_confirm + [username])
                 rows_updated = self._execute_local_sql(update_query, update_params, fetch_mode=None)
                 print(f"{rows_updated} registros marcados como sincronizados localmente.")
                 if rows_updated != len(saved_ids_confirm):
                      st.warning("Contagem de registros marcados localmente não bate com a contagem enviada.")
                 self._update_last_sync_time_gsheet(username)
                 remaining_unsaved = self.get_unsynced_documents_local(username)
                 st.session_state['unsaved_changes'] = bool(remaining_unsaved)
                 return True
             else: # Should not happen if docs_to_save was populated
                 st.warning("Nenhum ID confirmado para marcar como sincronizado localmente.")
                 return False
        except Exception as append_e:
             st.error(f"Falha ao anexar dados na planilha '{user_sheet_name}': {append_e}")
             return False

    def _update_last_sync_time_gsheet(self, username):
        users_ws = self._get_worksheet(config.SHEET_USERS)
        if not users_ws:
            st.error("Planilha 'usuarios' não encontrada para atualizar timestamp.")
            return False
        try:
            cell = users_ws.find(username, in_column=config.USERS_COLS.index('username') + 1)
            if not cell: return False 
            user_row_index = cell.row
            try:
                 timestamp_col_index = config.USERS_COLS.index('last_sync_timestamp') + 1
            except ValueError:
                 st.error("Coluna 'last_sync_timestamp' não definida em config.USERS_COLS.")
                 return False
            now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
            users_ws.update_cell(user_row_index, timestamp_col_index, now_str)
            self._execute_local_sql("UPDATE usuarios SET last_sync_timestamp = ? WHERE username = ?", (now_str, username), fetch_mode=None)
            return True
        except Exception as e:
            st.error(f"Erro ao atualizar timestamp para {username}: {e}")
            return False

    def update_document_status_gsheet_and_local(self, doc_id, new_status, admin_username, observacoes=""):
        """
        Updates the status, validation date, and validator for a specific document
        both in the corresponding Google Sheet and the local cache.
        """
        print(f"Attempting to update doc_id '{doc_id}' to status '{new_status}' by '{admin_username}'...")
        local_doc = self._execute_local_sql("SELECT colaborador_username, cliente_id FROM documentos WHERE id = ?", (doc_id,), fetch_mode="one")
        if not local_doc:
             st.error(f"Documento com ID '{doc_id}' não encontrado localmente.")
             return False

        colaborador_username = local_doc['colaborador_username']
        # cliente_id_from_doc = local_doc['cliente_id'] # May need this if GSheet doesn't have cliente_id
        user_sheet_name = self._get_user_sheet_name(colaborador_username)

        ws = self._get_worksheet(user_sheet_name)
        if not ws:
             st.error(f"Planilha '{user_sheet_name}' para o colaborador '{colaborador_username}' não encontrada.")
             return False
        try:
            id_col_gvar = 'id' # Name of ID col in GSheet
            header_values = ws.row_values(1) # Get header row
            if id_col_gvar not in header_values:
                st.error(f"Coluna '{id_col_gvar}' não encontrada no cabeçalho da planilha '{user_sheet_name}'.")
                return False
            id_col_index_gsheet = header_values.index(id_col_gvar) + 1
            
            cell = ws.find(doc_id, in_column=id_col_index_gsheet)
            if not cell:
                 st.error(f"Documento com ID '{doc_id}' não encontrado na planilha '{user_sheet_name}'.")
                 return False
            row_index = cell.row
            print(f"Found doc_id '{doc_id}' in sheet '{user_sheet_name}' at row {row_index}.")

            # Update GSheet columns based on their names in config.DOCS_COLS
            updates_batch = []
            now_str = datetime.now().isoformat(sep=' ', timespec='seconds')
            update_map = {
                'status': new_status,
                'data_validacao': now_str,
                'validado_por': admin_username,
                'observacoes_validacao': observacoes
            }

            for col_name, value_to_set in update_map.items():
                if col_name in header_values:
                    col_idx_gsheet = header_values.index(col_name) + 1
                    updates_batch.append({
                        'range': gspread.utils.rowcol_to_a1(row_index, col_idx_gsheet),
                        'values': [[value_to_set]]
                    })
                else:
                    print(f"Aviso: Coluna '{col_name}' não encontrada na planilha '{user_sheet_name}' durante a atualização do status.")

            if updates_batch:
                ws.batch_update(updates_batch, value_input_option='USER_ENTERED')
                print(f"GSheet row {row_index} updated (or attempted).")
            else:
                st.warning("Nenhuma coluna correspondente encontrada na planilha para atualização de status.")
                # Still update local if no GSheet cols match? Or fail? Let's update local.

            update_local_query = """
                UPDATE documentos
                SET status = ?, data_validacao = ?, validado_por = ?, observacoes_validacao = ?, is_synced = 1
                WHERE id = ?
            """
            rows_updated = self._execute_local_sql(
                update_local_query,
                (new_status, now_str, admin_username, observacoes, doc_id),
                fetch_mode=None
            )
            if rows_updated == 1:
                print(f"Local document ID '{doc_id}' updated successfully.")
                return True
            else: # Should not happen if local_doc was found
                st.error(f"Falha ao atualizar o registro local para o ID '{doc_id}' (linhas afetadas: {rows_updated}).")
                return False
        except gspread.exceptions.APIError as api_err:
             st.error(f"Erro de API do Google ao atualizar status para doc ID '{doc_id}': {api_err}")
        except ValueError as ve: 
             st.error(f"Erro de configuração ou planilha: coluna não encontrada. {ve}")
        except Exception as e:
             st.error(f"Erro inesperado ao atualizar status para doc ID '{doc_id}': {e}")
             import traceback; traceback.print_exc()
        return False

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
            
    def get_analise_cliente_data_local(self, cliente_id, colaborador_username=None, tipos_cliente_filter=None):
         """ Fetches data needed for the 'Análise por Cliente' donut charts, by cliente_id. """
         total_documentos_cliente = 0
         documentos_validados = 0
         documentos_nao_validados = 0 
         criterios_counts = {crit: 0 for crit in config.CRITERIA_COLORS.keys()}

         base_query = "SELECT d.status, d.dimensao_criterio FROM documentos d "
         conditions = ["d.cliente_id = ? COLLATE NOCASE"]
         params = [cliente_id]

         if colaborador_username:
             conditions.append("d.colaborador_username = ? COLLATE NOCASE")
             params.append(colaborador_username)
        
         # This filter seems redundant if cliente_id is already specified, but kept for consistency if needed.
         if tipos_cliente_filter and "Todos" not in tipos_cliente_filter and tipos_cliente_filter:
             base_query += " JOIN clientes c ON d.cliente_id = c.id " # Join needed if type filter is active
             if isinstance(tipos_cliente_filter, str):
                 tipos_cliente_filter = [tipos_cliente_filter]
             placeholders = ','.join('?'*len(tipos_cliente_filter))
             conditions.append(f"c.tipo IN ({placeholders})")
             params.extend(tipos_cliente_filter)
        
         if conditions:
             base_query += " WHERE " + " AND ".join(conditions)
        
         all_client_docs_results = self._execute_local_sql(base_query, tuple(params))

         if all_client_docs_results:
             for row in all_client_docs_results: # Each row is already a dict from sqlite3.Row
                 status = row['status']
                 dimensao = row['dimensao_criterio']

                 total_documentos_cliente +=1 # Count every document fetched for this client

                 if status == 'Validado':
                     documentos_validados += 1
                     if dimensao in criterios_counts: # Only count if 'Validado'
                         criterios_counts[dimensao] += 1
                 # else: # Any other status means it's not validated.
                     # documentos_nao_validados +=1 # This line is moved outside the loop for correct calculation

         documentos_nao_validados = total_documentos_cliente - documentos_validados
         
         # Ensure all criteria keys are present in the output even if count is 0
         for crit_key in config.CRITERIA_COLORS.keys():
             if crit_key not in criterios_counts:
                 criterios_counts[crit_key] = 0


         analise = {
             'total_documentos_cliente': total_documentos_cliente,
             'docs_validados': documentos_validados,
             'docs_invalidos': documentos_nao_validados, # Renamed for consistency with UI
             'criterios_counts': criterios_counts
         }
         return analise
    
    def get_assigned_clients_local(self, colaborador_username):
        """
        Gets list of client dicts {id, nome, tipo} assigned to a collaborator from local cache.
        Agora junta com a tabela clientes para obter todos os detalhes.
        """
        query = """
            SELECT c.id, c.nome, c.tipo
            FROM clientes c
            JOIN colaborador_cliente ca ON c.id = ca.cliente_id -- JUNÇÃO POR ID
            WHERE ca.colaborador_username = ? COLLATE NOCASE
            ORDER BY c.nome
        """
        results = self._execute_local_sql(query, (colaborador_username,))
        return [dict(row) for row in results] if results else []

    def get_assigned_clients_local(self, colaborador_username):
        """
        Gets list of client dicts {id, nome, tipo} assigned to a collaborator from local cache.
        Agora junta com a tabela clientes para obter todos os detalhes.
        """
        query = """
            SELECT c.id, c.nome, c.tipo
            FROM clientes c
            JOIN colaborador_cliente ca ON c.id = ca.cliente_id -- JUNÇÃO POR ID
            WHERE ca.colaborador_username = ? COLLATE NOCASE
            ORDER BY c.nome
        """
        results = self._execute_local_sql(query, (colaborador_username,))
        return [dict(row) for row in results] if results else []

    def assign_clients_to_collab(self, colaborador_username, client_ids_to_assign): # ACEITA IDs
        """Assigns clients (by ID) to a collaborator, updating local DB and GSheets."""
        if not colaborador_username or not client_ids_to_assign:
            st.warning("Nome de colaborador ou lista de IDs de clientes está vazia.")
            return False

        print(f"Atribuindo clientes com IDs {client_ids_to_assign} para {colaborador_username}...")
        assignments_to_add_gsheet = [] # Para a planilha, ainda [(username, client_id)]
        assign_success_count = 0
        assign_fail_count = 0

        with self.local_conn:
             cursor = self.local_conn.cursor()
             for cliente_id in client_ids_to_assign:
                  try:
                       cursor.execute("""
                           INSERT OR IGNORE INTO colaborador_cliente (colaborador_username, cliente_id)
                           VALUES (?, ?)
                       """, (colaborador_username, cliente_id)) # SALVA ID
                       if cursor.rowcount > 0:
                            assignments_to_add_gsheet.append([colaborador_username, cliente_id])
                       assign_success_count += 1
                  except sqlite3.Error as e:
                       print(f"Erro ao inserir atribuição local: {colaborador_username} -> ID {cliente_id}. Error: {e}")
                       assign_fail_count += 1
        
        if assignments_to_add_gsheet:
             ws = self._get_worksheet(config.SHEET_ASSOC)
             if ws:
                  try:
                       # Assume que SHEET_ASSOC agora espera [colaborador_username, cliente_id]
                       ws.append_rows(assignments_to_add_gsheet, value_input_option='USER_ENTERED')
                       print(f"{len(assignments_to_add_gsheet)} novas atribuições (ID) adicionadas à planilha '{config.SHEET_ASSOC}'.")
                  except Exception as e:
                       st.error(f"Erro ao salvar atribuições (ID) na planilha '{config.SHEET_ASSOC}': {e}")
                       return False
             else:
                  st.error(f"Planilha de atribuições '{config.SHEET_ASSOC}' não encontrada. Atribuições não salvas na nuvem.")
                  return False
        if assign_fail_count > 0:
            st.error(f"{assign_fail_count} atribuições falharam ao salvar localmente.")
            return False
        # st.success(f"{assign_success_count} atribuições (ID) salvas/verificadas com sucesso.") # Removido para evitar muitos toasts
        return True

    def unassign_clients_from_collab(self, colaborador_username, client_ids_to_unassign): # ACEITA IDs
        """Removes client assignments (by ID) for a collaborator (local and GSheets)."""
        if not colaborador_username or not client_ids_to_unassign:
             st.warning("Nome de colaborador ou lista de IDs de clientes para desatribuir está vazia.")
             return False

        print(f"Removendo atribuições de IDs {client_ids_to_unassign} de {colaborador_username}...")
        local_delete_count = 0
        with self.local_conn:
             cursor = self.local_conn.cursor()
             placeholders = ','.join('?' * len(client_ids_to_unassign))
             params = [colaborador_username] + client_ids_to_unassign
             cursor.execute(f"""
                 DELETE FROM colaborador_cliente
                 WHERE colaborador_username = ? COLLATE NOCASE
                 AND cliente_id IN ({placeholders}) -- COMPARA POR ID
             """, tuple(params))
             local_delete_count = cursor.rowcount

        if local_delete_count == 0 and client_ids_to_unassign:
             st.warning("Nenhuma atribuição (por ID) encontrada localmente para remover.")
        
        ws = self._get_worksheet(config.SHEET_ASSOC)
        if ws:
            try:
                # A remoção da GSheet agora precisa encontrar linhas baseadas em (colaborador_username, cliente_id)
                # Isso requer que a GSheet 'SHEET_ASSOC' tenha 'cliente_id'
                all_assoc_records_gsheet = ws.get_all_records(head=1) # Assume header na linha 1
                rows_to_delete_indices_gsheet = []

                # Encontrar as linhas para deletar na GSheet
                for i, record_gsheet in enumerate(all_assoc_records_gsheet):
                    record_collab_user = str(record_gsheet.get(config.ASSOC_COLS[0], '')).lower() # colaborador_username
                    record_client_val = str(record_gsheet.get(config.ASSOC_COLS[1], '')) # cliente_id

                    if record_collab_user == colaborador_username.lower() and record_client_val in client_ids_to_unassign:
                        rows_to_delete_indices_gsheet.append(i + 2) # +1 header, +1 0-based to 1-based

                if rows_to_delete_indices_gsheet:
                    print(f"Deletando {len(rows_to_delete_indices_gsheet)} linhas (por ID) da planilha '{config.SHEET_ASSOC}'...")
                    rows_to_delete_indices_gsheet.sort(reverse=True)
                    for row_idx_gsheet in rows_to_delete_indices_gsheet:
                        try:
                            ws.delete_rows(row_idx_gsheet)
                        except Exception as del_err_gsheet:
                            print(f"Erro ao deletar linha {row_idx_gsheet} (por ID) da planilha: {del_err_gsheet}")
                    print("Remoção (por ID) da planilha concluída (ou tentativas feitas).")
            except Exception as e_gsheet:
                st.error(f"Erro ao processar remoção (por ID) da planilha '{config.SHEET_ASSOC}': {e_gsheet}")
                return False
        elif local_delete_count > 0: # Se deletou localmente mas a planilha não foi encontrada
             st.error(f"Planilha de atribuições '{config.SHEET_ASSOC}' não encontrada. Atribuições removidas localmente, mas não da nuvem.")
             return False
        # st.success(f"{local_delete_count} atribuições (por ID) removidas com sucesso.") # Removido para evitar muitos toasts
        return True


    def add_cliente_local_and_gsheet(self, nome, tipo):
        if self._execute_local_sql("SELECT id FROM clientes WHERE nome = ? COLLATE NOCASE", (nome,), fetch_mode="one"):
             st.error(f"Cliente '{nome}' já existe localmente.")
             return False
        ws_clients = self._get_worksheet(config.SHEET_CLIENTS)
        if ws_clients:
            try: # Check 'nome' in column 2 (index 1 of CLIENTS_COLS)
                 if config.CLIENTS_COLS.index('nome') + 1: # ensure 'nome' is in config
                    nome_col_idx = config.CLIENTS_COLS.index('nome') + 1
                    cell = ws_clients.find(nome, in_column=nome_col_idx)
                    if cell:
                         st.error(f"Cliente '{nome}' já existe na planilha.")
                         return False
            except Exception as e:
                 st.error(f"Erro ao verificar duplicidade de cliente na planilha: {e}")
                 return False # Or proceed with caution
        else:
             st.error(f"Planilha '{config.SHEET_CLIENTS}' não encontrada.")
             return False
        client_id = str(uuid.uuid4())
        add_local_success = False
        try:
            rowcount = self._execute_local_sql("INSERT INTO clientes (id, nome, tipo) VALUES (?, ?, ?)", (client_id, nome, tipo), fetch_mode=None)
            if rowcount == 1: add_local_success = True
        except sqlite3.Error as e: st.error(f"Erro SQLite ao adicionar cliente: {e}")
        if not add_local_success: return False
        try:
            client_data_ordered = [None] * len(config.CLIENTS_COLS) # Ensure correct order
            for i, col_name in enumerate(config.CLIENTS_COLS):
                if col_name == 'id': client_data_ordered[i] = client_id
                elif col_name == 'nome': client_data_ordered[i] = nome
                elif col_name == 'tipo': client_data_ordered[i] = tipo
            ws_clients.append_row(client_data_ordered, value_input_option='USER_ENTERED')
            st.success(f"Cliente '{nome}' ({tipo}) adicionado com sucesso.")
            return True
        except Exception as e:
            st.error(f"Cliente adicionado localmente, mas falha ao adicionar na planilha: {e}")
            # Consider rollback here
            return False

    def _hash_password(self, password):
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

class Autenticador:
    def __init__(self, db_manager: HybridDBManager):
        self.gerenciador_bd = db_manager

    def _hash_password(self, password):
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    def _verificar_senha(self, stored_hashed_password, provided_password):
        return stored_hashed_password == self._hash_password(provided_password)

    def login(self, username, password):
        print(f"Attempting login for {username}.")
        # Prioritize local cache for login check for speed after initial load from sheets.
        # However, the very first login must hit GSheets if local cache is empty.
        # The current _check_login_on_sheets always hits GSheets.
        
        success, user_info_or_error = self._check_login_on_sheets(username, password)

        if success:
             user_info = user_info_or_error 
             st.session_state['logged_in'] = True
             st.session_state['username'] = user_info['username']
             st.session_state['role'] = user_info['role']
             st.session_state['nome_completo'] = user_info['nome_completo']
             st.session_state['cliente_nome'] = None 

             if user_info['role'] == 'Cliente':
                  # For 'Cliente' role, their username IS the client's name.
                  # We also need to get their client_id
                  cliente_obj = self.gerenciador_bd._execute_local_sql(
                      "SELECT id, nome FROM clientes WHERE nome = ? COLLATE NOCASE",
                      (user_info['username'],), fetch_mode="one"
                  )
                  if cliente_obj:
                      st.session_state['cliente_nome'] = cliente_obj['nome'] # Storing name
                      st.session_state['cliente_id_logado'] = cliente_obj['id'] # Storing ID
                      print(f"Client login: {cliente_obj['nome']}, ID: {cliente_obj['id']}")
                  else: # Should not happen if client user exists and clients table is synced
                      st.error(f"Informação do cliente '{user_info['username']}' não encontrada.")
                      self._clear_session()
                      return False, "Erro de configuração do cliente."
             try:
                  manager_instance = st.session_state.get('db_manager')
                  if not manager_instance:
                       st.error("Critical Error: DB Manager not found in session state during login.")
                       self._clear_session()
                       return False, "Internal server error during login."
                  manager_instance.load_data_for_session(user_info['username'], user_info['role'])
             except Exception as load_e:
                  st.error(f"Failed to load data after login: {load_e}")
                  self._clear_session() 
                  return False, "Data loading error."
             return True, "Login e carregamento de dados bem-sucedidos."
        else:
            return False, user_info_or_error

    def _clear_session(self):
        keys_to_clear = [
            'logged_in', 'username', 'role', 'nome_completo', 
            'cliente_nome', 'cliente_id_logado', # Added cliente_id_logado
            'data_loaded', 'last_load_time', 'unsaved_changes'
            # 'db_manager' is typically kept
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        print(f"Cleared session keys for logout/error.")

    def logout(self):
        self._clear_session()
        print("User logged out.")
        st.cache_resource.clear() 
        st.cache_data.clear()    
        st.rerun() 

    def _check_login_on_sheets(self, username, password):
        users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
        if not users_ws: return False, "Error: User worksheet not accessible."
        try:
              user_data_list = users_ws.get_all_records()
              user_data = next((record for record in user_data_list
                                if str(record.get('username','')).strip().lower() == str(username).strip().lower()), None)
              if user_data and isinstance(user_data, dict):
                   stored_hash = user_data.get('hashed_password')
                   if stored_hash and self._verificar_senha(stored_hash, password):
                        return True, dict(user_data)
                   else: return False, "Senha incorreta."
              else: return False, "Usuário não encontrado."
        except Exception as e:
              st.error(f"Error verifying user in the sheet: {e}")
              return False, "Error during login attempt."

    def add_default_admin_if_needed(self):
        users_ws = self.gerenciador_bd._get_worksheet(config.SHEET_USERS)
        if not users_ws:
            print("Warning: Cannot check/add default admin, user sheet not found.")
            return
        try:
            data = users_ws.get_all_records() 
            admin_exists = any(str(record.get('username','')).strip() == config.DEFAULT_ADMIN_USER for record in data)
            if not admin_exists:
                print(f"Admin '{config.DEFAULT_ADMIN_USER}' not found. Adding to GSheet...")
                hashed_pw = self._hash_password(config.DEFAULT_ADMIN_PASS)
                admin_data_row = [None] * len(config.USERS_COLS)
                for i, col_name in enumerate(config.USERS_COLS):
                    if col_name == 'username': admin_data_row[i] = config.DEFAULT_ADMIN_USER
                    elif col_name == 'hashed_password': admin_data_row[i] = hashed_pw
                    elif col_name == 'nome_completo': admin_data_row[i] = "Administrador Padrão"
                    elif col_name == 'role': admin_data_row[i] = "Admin"
                    # last_sync_timestamp can be None or empty string initially
                users_ws.append_row(admin_data_row, value_input_option='USER_ENTERED')
                print("Default admin added to the sheet.")
        except Exception as e:
             print(f"Error checking/adding default admin: {e}")