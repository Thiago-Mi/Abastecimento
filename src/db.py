# --- START OF FILE db.py ---

import streamlit as st
import sqlite3
import hashlib
import os
import pandas as pd
from datetime import datetime, timedelta, date # Adicionado date
import random
import config # Importa o novo arquivo de configuração

# --- Configuração Inicial ---
# DB_FILE = "app_database.db" # Removido - agora vem do config
DB_FILE = config.DB_FILE

class GerenciadorBD:
    def __init__(self, db_file):
        """Inicializa o gerenciador e conecta ao banco de dados."""
        self.db_file = db_file
        self._criar_tabelas_se_nao_existir()
        self._adicionar_admin_padrao_se_necessario()
        # Descomente para popular na primeira execução se o DB estiver vazio
        self._popular_dados_exemplo() # << LINHA DESCOMENTADA AQUI
    
    @st.cache_data
    def _conectar(self):
        """Retorna uma conexão com o banco de dados."""
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _criar_tabelas_se_nao_existir(self):
        """Cria as tabelas necessárias se elas ainda não existirem."""
        conn = self._conectar()
        cursor = conn.cursor()
        try:
            # Tabela de Usuários
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    username TEXT PRIMARY KEY UNIQUE NOT NULL,
                    hashed_password TEXT NOT NULL,
                    nome_completo TEXT,
                    role TEXT NOT NULL CHECK(role IN ('Admin', 'Usuario', 'Cliente'))
                )
            """)
            # Tabela de Clientes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clientes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT UNIQUE NOT NULL,
                    tipo TEXT
                )
            """)
            # Tabela de Associação Colaborador <-> Cliente
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS colaborador_cliente (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    colaborador_username TEXT NOT NULL,
                    cliente_id INTEGER NOT NULL,
                    FOREIGN KEY (colaborador_username) REFERENCES usuarios(username) ON DELETE CASCADE,
                    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE,
                    UNIQUE (colaborador_username, cliente_id)
                )
            """)
            # Tabela de Documentos/Links
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS documentos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    colaborador_username TEXT NOT NULL,
                    cliente_id INTEGER NOT NULL,
                    descricao_ou_link TEXT NOT NULL,
                    tipo_documento TEXT,
                    status TEXT NOT NULL CHECK(status IN ('Enviado', 'Validado', 'Pendente', 'Inválido')),
                    data_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_atualizacao TIMESTAMP,
                    FOREIGN KEY (colaborador_username) REFERENCES usuarios(username) ON DELETE CASCADE,
                    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
                )
            """)
            # Índices
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_colaborador ON documentos(colaborador_username)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_cliente ON documentos(cliente_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_doc_status ON documentos(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assoc_colaborador ON colaborador_cliente(colaborador_username)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assoc_cliente ON colaborador_cliente(cliente_id)")
            conn.commit()
        except sqlite3.Error as e:
            print(f"Erro ao criar tabela: {e}")
        finally:
            conn.close()


    def _adicionar_admin_padrao_se_necessario(self):
         # ... (sem mudanças) ...
        conn = self._conectar()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM usuarios")
            count = cursor.fetchone()[0]
            if count == 0:
                hashed_pw = self._hash_password("admin")
                cursor.execute("""
                    INSERT INTO usuarios (username, hashed_password, nome_completo, role)
                    VALUES (?, ?, ?, ?)
                """, ("admin", hashed_pw, "Administrador", "Admin"))
                conn.commit()
                print("Usuário 'admin' padrão criado com senha 'admin'.")
        except sqlite3.Error as e: print(f"Erro ao adicionar admin padrão: {e}")
        finally: conn.close()

    def _hash_password(self, password): # ...
        return hashlib.sha256(password.encode('utf-8')).hexdigest()

    def adicionar_usuario(self, username, password, nome_completo, role): # ...
        if role not in ['Admin', 'Usuario', 'Cliente']: return False, f"Role '{role}' inválido."
        hashed_pw = self._hash_password(password)
        conn = self._conectar(); cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO usuarios VALUES (?, ?, ?, ?)", (username, hashed_pw, nome_completo, role))
            conn.commit(); return True, "Usuário adicionado com sucesso."
        except sqlite3.IntegrityError: return False, f"Erro: Usuário '{username}' já existe."
        except sqlite3.Error as e: print(f"Erro add usuário: {e}"); return False, f"Erro: {e}"
        finally: conn.close()


    def buscar_usuario(self, username): # ...
        conn = self._conectar(); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM usuarios WHERE username = ?", (username,))
            return cursor.fetchone()
        except sqlite3.Error as e: print(f"Erro buscar usuário: {e}"); return None
        finally: conn.close()

    @st.cache_data
    def listar_colaboradores(self): # ...
        conn = self._conectar(); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
        try:
            cursor.execute("SELECT username, nome_completo FROM usuarios WHERE role = 'Usuario' ORDER BY nome_completo")
            return cursor.fetchall()
        except sqlite3.Error as e: print(f"Erro listar colab: {e}"); return []
        finally: conn.close()

    def adicionar_cliente(self, nome, tipo): # ... (sem mudanças)
        conn = self._conectar(); cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO clientes (nome, tipo) VALUES (?, ?)", (nome, tipo))
            conn.commit(); return True, "Cliente adicionado com sucesso."
        except sqlite3.IntegrityError: return False, f"Erro: Cliente '{nome}' já existe."
        except sqlite3.Error as e: print(f"Erro add cliente: {e}"); return False, f"Erro: {e}"
        finally: conn.close()
    
    @st.cache_data
    def listar_clientes(self, colaborador_username=None, tipo_cliente=None, cliente_id=None): # Add cliente_id filter
        """Lista clientes, opcionalmente filtrados."""
        conn = self._conectar()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            query = "SELECT DISTINCT c.id, c.nome, c.tipo FROM clientes c"
            params = []
            conditions = []

            if colaborador_username:
                query += " JOIN colaborador_cliente cc ON c.id = cc.cliente_id"
                conditions.append("cc.colaborador_username = ?")
                params.append(colaborador_username)

            if tipo_cliente:
                conditions.append("c.tipo = ?")
                params.append(tipo_cliente)

            if cliente_id: # Novo filtro
                 conditions.append("c.id = ?")
                 params.append(cliente_id)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY c.nome"
            cursor.execute(query, tuple(params))
            return cursor.fetchall()

        except sqlite3.Error as e:
            print(f"Erro ao listar clientes: {e}")
            return []
        finally:
            conn.close()
    
    @st.cache_data
    def listar_tipos_cliente(self, colaborador_username=None): # ... (sem mudanças)
         conn = self._conectar(); cursor = conn.cursor()
         try:
             query = "SELECT DISTINCT c.tipo FROM clientes c"; params = []
             if colaborador_username:
                 query += " JOIN colaborador_cliente cc ON c.id = cc.cliente_id WHERE cc.colaborador_username = ?"
                 params.append(colaborador_username)
                 query += " AND c.tipo IS NOT NULL AND c.tipo != '' ORDER BY c.tipo"
             else:
                 query += " WHERE c.tipo IS NOT NULL AND c.tipo != '' ORDER BY c.tipo"
             cursor.execute(query, tuple(params))
             return [row[0] for row in cursor.fetchall()]
         except sqlite3.Error as e: print(f"Erro listar tipos: {e}"); return []
         finally: conn.close()
    @st.cache_data
    def buscar_cliente_por_nome(self, nome):
        """Busca um cliente pelo nome exato."""
        conn = self._conectar()
        conn.row_factory = sqlite3.Row # Para retornar como dicionário
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, nome, tipo FROM clientes WHERE nome = ?", (nome,))
            return cursor.fetchone() # Retorna Row ou None
        except sqlite3.Error as e:
            print(f"Erro ao buscar cliente por nome: {e}")
            return None
        finally:
            conn.close()

    @st.cache_data
    def buscar_cliente_por_id(self, cliente_id):
        """Busca um cliente pelo ID."""
        if cliente_id is None:
            return None
        conn = self._conectar()
        conn.row_factory = sqlite3.Row # Para retornar como dicionário
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id, nome, tipo FROM clientes WHERE id = ?", (cliente_id,))
            return cursor.fetchone() # Retorna Row ou None
        except sqlite3.Error as e:
            print(f"Erro ao buscar cliente por ID: {e}")
            return None
        finally:
            conn.close()

    def associar_colaborador_cliente(self, colaborador_username, cliente_id): # ... (sem mudanças)
        conn = self._conectar(); cursor = conn.cursor()
        try:
            cursor.execute("INSERT INTO colaborador_cliente (colaborador_username, cliente_id) VALUES (?, ?)", (colaborador_username, cliente_id))
            conn.commit(); return True, "Associação realizada com sucesso."
        except sqlite3.IntegrityError: return False, "Erro: Associação já existe ou usuário/cliente inválido."
        except sqlite3.Error as e: print(f"Erro ao associar: {e}"); return False, f"Erro: {e}"
        finally: conn.close()


    def adicionar_documento(self, colab, cliente_id, desc, tipo, status='Enviado'): # ...
        conn = self._conectar(); cursor = conn.cursor(); now = datetime.now()
        try:
            cursor.execute("INSERT INTO documentos (colaborador_username, cliente_id, descricao_ou_link, tipo_documento, status, data_envio, data_atualizacao) VALUES (?, ?, ?, ?, ?, ?, ?)",
                           (colab, cliente_id, desc, tipo, status, now, now))
            conn.commit(); return True, "Doc adicionado."
        except sqlite3.Error as e: print(f"Erro add doc: {e}"); return False, f"Erro: {e}"
        finally: conn.close()

    def atualizar_status_documento(self, doc_id, status): # ...
        # Define valid_statuses explicitamente ou importa de config
        valid_statuses = ['Enviado', 'Validado', 'Pendente', 'Inválido']
        if status not in valid_statuses: return False, "Status inválido."
        conn = self._conectar(); cursor = conn.cursor(); now = datetime.now()
        try:
            cursor.execute("UPDATE documentos SET status = ?, data_atualizacao = ? WHERE id = ?", (status, now, doc_id))
            conn.commit(); return bool(cursor.rowcount), "Status atualizado." if cursor.rowcount else "Doc não encontrado."
        except sqlite3.Error as e: print(f"Erro att status: {e}"); return False, f"Erro: {e}"
        finally: conn.close()
    
    @st.cache_data
    def get_kpi_data_cliente(self, cliente_id, periodo_dias=None):
        """Busca dados para os KPIs do painel do cliente (Enviados, Publicados, Pendentes)."""
        conn = self._conectar(); cursor = conn.cursor()
        kpi = {'enviados': 0, 'publicados': 0, 'pendentes': 0}
        if cliente_id is None: return kpi

        try:
            query = "SELECT status, COUNT(*) FROM documentos WHERE cliente_id = ?"
            params = [cliente_id]
            if periodo_dias:
                 data_inicio = datetime.now() - timedelta(days=periodo_dias)
                 query += " AND data_envio >= ?" # Ou data_atualizacao?
                 params.append(data_inicio)
            query += " GROUP BY status"
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            status_map_cliente = {'Validado': 'publicados', 'Enviado': 'enviados', 'Pendente': 'pendentes'}
            for status, count in results:
                if status in status_map_cliente: kpi[status_map_cliente[status]] = count
        except sqlite3.Error as e: print(f"Erro KPI Cliente: {e}")
        finally: conn.close()
        return kpi
    
    @st.cache_data
    def get_kpi_data(self, colaborador_username=None):
        """Busca dados para os KPIs (cards). Se colaborador_username for None, busca dados globais."""
        conn = self._conectar()
        cursor = conn.cursor()
        kpi = {'enviados': 0, 'validados': 0, 'pendentes': 0, 'invalidos': 0}
        try:
            base_query = "SELECT status, COUNT(*) FROM documentos"
            params = []
            if colaborador_username:
                base_query += " WHERE colaborador_username = ?"
                params.append(colaborador_username)
            query = f"{base_query} GROUP BY status"
            cursor.execute(query, tuple(params))
            results = cursor.fetchall()
            status_map = {'Enviado': 'enviados', 'Validado': 'validados', 'Pendente': 'pendentes', 'Inválido': 'invalidos'}
            for status, count in results:
                if status in status_map: kpi[status_map[status]] = count
        except sqlite3.Error as e: print(f"Erro ao buscar dados KPI: {e}")
        finally: conn.close()
        return kpi
    
    @st.cache_data
    def get_docs_por_periodo_cliente(self, cliente_id, grupo='W'): # W=Semana, D=Dia, M=Mês
        """Busca a contagem de documentos (Validados?) agrupados por período para o gráfico de linha."""
        df_result = pd.DataFrame({'periodo': [], 'contagem': [], 'periodo_dt': []}) # Inclui periodo_dt
        if cliente_id is None: return df_result
        conn = self._conectar(); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
        format_map = {'W': '%Y-%W', 'D': '%Y-%m-%d', 'M': '%Y-%m'}
        sql_format = format_map.get(grupo, '%Y-%W') # Padrão Semanal
        try:
            # Assume data_atualizacao para contagem (ou data_envio?)
            cursor.execute(f"""SELECT strftime('{sql_format}', COALESCE(data_atualizacao, data_envio)) as periodo,
                                     COUNT(*) as contagem
                                FROM documentos
                                WHERE cliente_id = ? AND status = 'Validado'
                                GROUP BY periodo HAVING periodo IS NOT NULL ORDER BY periodo ASC""", (cliente_id,))
            resultados = cursor.fetchall()
            if resultados:
                 try:
                     df_result = pd.DataFrame([dict(row) for row in resultados])
                     if grupo == 'W':
                          df_result['periodo_dt'] = pd.to_datetime(df_result['periodo'] + '-1', format='%Y-%W-%w')
                     elif grupo == 'M':
                          df_result['periodo_dt'] = pd.to_datetime(df_result['periodo'] + '-01')
                     else:
                          df_result['periodo_dt'] = pd.to_datetime(df_result['periodo'])
                     df_result = df_result.sort_values('periodo_dt')
                 except Exception as e_pd: print(f"Erro conversão Pandas linha tempo: {e_pd}")
        except sqlite3.Error as e: print(f"Erro docs por período: {e}")
        finally: conn.close()
        return df_result
    
    @st.cache_data
    def get_criterios_atendidos_cliente(self, cliente_id):
        """Busca a contagem total e atendida (validada) de documentos por critério."""
        crit_data = {}
        if cliente_id is None: return crit_data
        conn = self._conectar(); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
        tipos_criterio = ['Critérios Essenciais', 'Obrigatórios', 'Recomendados']
        for crit in tipos_criterio: crit_data[crit] = {'total': 0, 'atendidos': 0}
        try:
            cursor.execute("""SELECT tipo_documento, COUNT(*) as total_docs,
                                     SUM(CASE WHEN status = 'Validado' THEN 1 ELSE 0 END) as docs_validados
                                FROM documentos WHERE cliente_id = ? AND tipo_documento IN (?, ?, ?)
                                GROUP BY tipo_documento""",
                           (cliente_id, *tipos_criterio)) # Unpacking dos tipos
            resultados = cursor.fetchall()
            for row in resultados:
                 tipo = row['tipo_documento']
                 if tipo in crit_data:
                     crit_data[tipo]['total'] = row['total_docs'] or 0
                     crit_data[tipo]['atendidos'] = row['docs_validados'] or 0
        except sqlite3.Error as e: print(f"Erro critérios atendidos: {e}")
        finally: conn.close()
        return crit_data
    
    @st.cache_data
    def calcular_pontuacao_colaboradores(self):
        """Calcula a pontuação, contagem e percentual de links validados dos colaboradores."""
        conn = self._conectar()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        df_pontuacao = pd.DataFrame({'Colaborador': [], 'Pontuação': [], 'Links Validados': [], 'Percentual': []}).set_index('Colaborador')

        try:
            cursor.execute("""SELECT u.nome_completo, SUM(CASE WHEN d.status = 'Validado' THEN 1 ELSE 0 END) as links_validados,
                                     COUNT(d.id) as total_links -- Adicionado para calcular %
                                FROM documentos d
                                JOIN usuarios u ON d.colaborador_username = u.username
                                WHERE u.role = 'Usuario'
                                GROUP BY u.nome_completo -- Agrupa pelo nome completo para o DF final
                                ORDER BY links_validados DESC, u.nome_completo ASC""")
            resultados = cursor.fetchall()

            if resultados:
                data = []
                total_validados_geral = sum(row['links_validados'] for row in resultados if row['links_validados'])
                for row in resultados:
                    nome = row['nome_completo']
                    validados = row['links_validados'] if row['links_validados'] else 0
                    #total_colab = row['total_links'] if row['total_links'] else 0 # Total por colaborador (não usado diretamente no grafico atual)
                    pontuacao = validados * 10
                    percentual_geral = (validados / total_validados_geral * 100) if total_validados_geral > 0 else 0.0
                    data.append({
                        'Colaborador': nome, 'Pontuação': pontuacao,
                        'Links Validados': validados, 'Percentual': percentual_geral
                    })
                if data:
                    df_pontuacao = pd.DataFrame(data).set_index('Colaborador')
                    # Ordenação já feita na query SQL
        except sqlite3.Error as e: print(f"Erro ao calcular pontuação: {e}")
        finally: conn.close()
        return df_pontuacao
    
    @st.cache_data
    def get_analise_cliente_data(self, cliente_id, colaborador_username=None):
        """Busca dados para a seção 'Análise por Cliente' do dashboard Admin/Usuario."""
        conn = self._conectar(); conn.row_factory = sqlite3.Row; cursor = conn.cursor()
        analise = {'docs_drive': 54, 'docs_publicados': 0, 'docs_pendentes': 36, 'criterios_counts': {}} # Placeholders
        if not cliente_id: return analise
        try:
            query_publicados = "SELECT COUNT(*) FROM documentos WHERE cliente_id = ? AND status = 'Validado'"
            params_publicados = [cliente_id]
            if colaborador_username: query_publicados += " AND colaborador_username = ?"; params_publicados.append(colaborador_username)
            cursor.execute(query_publicados, tuple(params_publicados))
            result_pub = cursor.fetchone(); analise['docs_publicados'] = result_pub[0] if result_pub else 0
            analise['docs_pendentes'] = max(0, analise['docs_drive'] - analise['docs_publicados'])

            query_tipos = "SELECT tipo_documento, COUNT(*) FROM documentos WHERE cliente_id = ? AND tipo_documento IS NOT NULL AND tipo_documento != ''"
            params_tipos = [cliente_id]
            if colaborador_username: query_tipos += " AND colaborador_username = ?"; params_tipos.append(colaborador_username)
            query_tipos += " GROUP BY tipo_documento"
            cursor.execute(query_tipos, tuple(params_tipos))
            results_tipos = cursor.fetchall()
            crit_counts = {}
            for tipo, count in results_tipos: crit_counts[tipo] = count
            analise['criterios_counts'] = crit_counts.copy() # Assume que os tipos são os critérios
        except sqlite3.Error as e: print(f"Erro análise cliente: {e}")
        finally: conn.close()
        return analise

    def _popular_dados_exemplo(self):
        """Adiciona dados de exemplo (já implementado anteriormente, pode ajustar se necessário)."""
        print("Verificando/Populando dados de exemplo...")
        conn = self._conectar()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM documentos")
            doc_count = cursor.fetchone()[0]
            if doc_count > 50: print("Dados já existem."); conn.close(); return # Fecha conexão antes de retornar

            print("Populando/Atualizando dados de exemplo...")
            # Adicionar/Garantir usuários (incluindo cliente com username)
            users = [
                ('diogo', 'senha123', 'Diogo Avanzi', 'Usuario'),
                ('lorena', 'senha123', 'Lorena Victória', 'Usuario'),
                ('paulo', 'senha123', 'Paulo Sérgio', 'Usuario'),
                ('cliente_msj', 'cliente', 'Mata de São João', 'Cliente'),
                ('cliente_camacari', 'cliente', 'Camaçari', 'Cliente'),
                # Adicione admin aqui também se _adicionar_admin_padrao não rodou
                # ('admin', 'admin', 'Admin Master', 'Admin')
            ]
            colaborador_usernames = []
            for u in users:
                try:
                    cursor.execute("INSERT OR IGNORE INTO usuarios VALUES (?, ?, ?, ?)", (u[0], self._hash_password(u[1]), u[2], u[3]))
                    if u[3] == 'Usuario': colaborador_usernames.append(u[0])
                except sqlite3.Error as e: print(f"Ignorando erro ao inserir user {u[0]}: {e}")

            # Adicionar/Garantir clientes
            clients = [
                ('Mata de São João', 'Prefeitura'), ('Camaçari', 'Prefeitura'),
                ('Lauro de Freitas', 'Câmara'), ('Feira de Santana', 'Autarquia'),
                ('Salvador', 'Prefeitura'), ('Ilhéus', 'Prefeitura'), ('Itabuna', 'Câmara')
            ]
            client_name_to_id = {}
            for c_nome, c_tipo in clients:
                try:
                    cursor.execute("INSERT OR IGNORE INTO clientes (nome, tipo) VALUES (?, ?)", (c_nome, c_tipo))
                    cursor.execute("SELECT id FROM clientes WHERE nome = ?", (c_nome,))
                    c_id = cursor.fetchone()
                    if c_id: 
                        client_name_to_id[c_nome] = c_id[0]
                        print(client_name_to_id[c_nome], c_nome, client_name_to_id)
                except sqlite3.Error as e: print(f"Ignorando erro ao inserir cliente {c_nome}: {e}")

            # Associações (Garante algumas associações chave)
            associations_tuples = [
                 ('diogo', client_name_to_id.get('Mata de São João')),
                 ('paulo', client_name_to_id.get('Mata de São João')),
                 ('diogo', client_name_to_id.get('Camaçari')),
                 ('lorena', client_name_to_id.get('Lauro de Freitas')),
            ]
            valid_associations = [a for a in associations_tuples if a[1] is not None] # Filtra Nones
            try: cursor.executemany("INSERT OR IGNORE INTO colaborador_cliente VALUES (NULL, ?, ?)", valid_associations)
            except sqlite3.Error as e: print(f"Ignorando erro ao inserir associações: {e}")

            # Adicionar Documentos para clientes específicos
            docs_to_add_list = []
            tipos_doc = ['Critérios Essenciais', 'Obrigatórios', 'Recomendados']
            status_opts = ['Validado', 'Pendente', 'Enviado', 'Inválido']

            # Adiciona documentos para Mata de São João
            cliente_msj_id = client_name_to_id.get('Mata de São João')
            if cliente_msj_id and colaborador_usernames:
                 for i in range(random.randint(50, 100)):
                     colab = random.choice(['diogo', 'paulo']) if 'diogo' in colaborador_usernames and 'paulo' in colaborador_usernames else random.choice(colaborador_usernames)
                     tipo = random.choice(tipos_doc)
                     status = random.choices(status_opts, weights=[50, 20, 20, 10], k=1)[0]
                     delta = random.randint(1, 120); data = datetime.now() - timedelta(days=delta)
                     desc = f"Doc MSJ {tipo} {i}"
                     docs_to_add_list.append((colab, cliente_msj_id, desc, tipo, status, data, data))

            # Adiciona documentos para Camaçari
            cliente_cam_id = client_name_to_id.get('Camaçari')
            if cliente_cam_id and colaborador_usernames:
                 for i in range(random.randint(40, 80)):
                     colab = 'diogo' if 'diogo' in colaborador_usernames else random.choice(colaborador_usernames)
                     tipo = random.choice(tipos_doc)
                     status = random.choices(status_opts, weights=[60, 15, 15, 10], k=1)[0]
                     delta = random.randint(1, 150); data = datetime.now() - timedelta(days=delta)
                     desc = f"Doc CAM {tipo} {i}"
                     docs_to_add_list.append((colab, cliente_cam_id, desc, tipo, status, data, data))

            # Adiciona alguns documentos gerais
            if client_name_to_id and colaborador_usernames:
                for _ in range(150):
                    colab = random.choice(colaborador_usernames)
                    cliente_id = random.choice(list(client_name_to_id.values()))
                    tipo = random.choice(tipos_doc)
                    status = random.choices(status_opts, weights=[40, 25, 25, 10], k=1)[0]
                    delta = random.randint(1, 180); data = datetime.now() - timedelta(days=delta)
                    desc = f"Doc Geral {tipo} {random.randint(1000, 9999)}"
                    docs_to_add_list.append((colab, cliente_id, desc, tipo, status, data, data))


            if docs_to_add_list:
                 try: cursor.executemany("""INSERT INTO documentos (colaborador_username, cliente_id, descricao_ou_link, tipo_documento, status, data_envio, data_atualizacao)
                                             VALUES (?, ?, ?, ?, ?, ?, ?)""", docs_to_add_list)
                 except sqlite3.Error as e: print(f"Ignorando erro ao inserir docs: {e}")


            conn.commit()
            print("Dados de exemplo populados/verificados.")

        except sqlite3.Error as e: print(f"Erro geral ao pop. dados: {e}"); conn.rollback()
        finally: conn.close() # Garante que a conexão seja fechada

# Classe Autenticador
class Autenticador:
    def __init__(self, gerenciador_bd):
        self.gerenciador_bd = gerenciador_bd

    def _verificar_senha(self, stored_hashed_password, provided_password):
        return stored_hashed_password == self.gerenciador_bd._hash_password(provided_password)

    def login(self, username, password):
        user_data = self.gerenciador_bd.buscar_usuario(username)
        if user_data:
            stored_hashed_password = user_data['hashed_password']
            if self._verificar_senha(stored_hashed_password, password):
                # Limpa chaves existentes exceto 'logged_in' se existir
                keys_to_clear = [k for k in st.session_state.keys() if k != 'logged_in']
                for key in keys_to_clear: del st.session_state[key]

                # Define o estado do usuário logado
                st.session_state['logged_in'] = True
                st.session_state['username'] = user_data['username']
                st.session_state['role'] = user_data['role']
                st.session_state['nome_completo'] = user_data['nome_completo']

                # Lógica para Cliente
                if user_data['role'] == 'Cliente':
                    cliente_info = self.gerenciador_bd.buscar_cliente_por_nome(user_data['nome_completo'])
                    if not cliente_info: cliente_info = self.gerenciador_bd.buscar_cliente_por_nome(user_data['username'])
                    if cliente_info:
                        st.session_state['cliente_id'] = cliente_info['id']
                        st.session_state['cliente_nome'] = cliente_info['nome']
                    else:
                        st.session_state['cliente_id'] = None
                        st.session_state['cliente_nome'] = user_data['nome_completo']
                        print(f"Aviso: Cliente '{username}' não encontrado na tabela clientes.")

                return True, "Login bem-sucedido."
            else:
                return False, "Senha incorreta."
        else:
            return False, "Usuário não encontrado."

    def logout(self):
        # Limpa todas as chaves ao fazer logout
        keys_to_clear = list(st.session_state.keys())
        for key in keys_to_clear:
            del st.session_state[key]
        # Garante que logged_in seja False após limpar
        st.session_state['logged_in'] = False
        st.rerun()

# --- END OF FILE db.py ---