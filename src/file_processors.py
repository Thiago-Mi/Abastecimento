# --- START OF FILE file_processors.py ---

import pandas as pd
import streamlit as st # Ainda precisamos do st para st.error em caso de exceção geral
from io import StringIO, BytesIO
import sqlite3 # Para type hinting e exceções específicas do DB
from db import GerenciadorBD # Importa a classe para type hinting
import config # Importa as constantes

def processar_arquivo_txt_usuarios(gerenciador_bd: GerenciadorBD, uploaded_file):
    """Lê, valida e tenta adicionar usuários de um arquivo TXT."""
    results = {'success': 0, 'failed': 0, 'errors': []}

    try:
        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
        lines = stringio.readlines()

        for i, line in enumerate(lines):
            line_num = i + 1
            line = line.strip()
            if not line or line.startswith('#'): continue

            parts = line.split(',')
            if len(parts) != 4:
                results['failed'] += 1; results['errors'].append(f"Linha {line_num}: Formato inválido. Partes: {len(parts)}"); continue

            username, fullname, password, role_str = [p.strip() for p in parts]
            role = role_str.capitalize()

            if not all([username, fullname, password, role]):
                results['failed'] += 1; results['errors'].append(f"Linha {line_num}: Campos vazios."); continue

            if role not in config.VALID_UPLOAD_ROLES:
                results['failed'] += 1; results['errors'].append(f"Linha {line_num}: Papel '{role_str}' inválido. Válidos: {config.VALID_UPLOAD_ROLES}"); continue

            success_db, message_db = gerenciador_bd.adicionar_usuario(username, password, fullname, role)
            if success_db: results['success'] += 1
            else: results['failed'] += 1; results['errors'].append(f"Linha {line_num} ({username}): {message_db}")

    except Exception as e:
        st.error(f"Erro inesperado ao processar arquivo TXT de usuários: {e}")
        results = {'success': 0, 'failed': -1, 'errors': [f"Erro geral no processamento: {e}"]}

    return results

def processar_arquivo_clientes(gerenciador_bd: GerenciadorBD, uploaded_file):
    """Lê, valida e tenta adicionar clientes de um arquivo CSV ou XLSX."""
    results = {'success': 0, 'failed': 0, 'errors': []}
    required_cols = config.CLIENT_UPLOAD_REQUIRED_COLS

    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(('.xls', '.xlsx')):
            excel_data = BytesIO(uploaded_file.getvalue())
            df = pd.read_excel(excel_data, engine='openpyxl')
        else:
            results['errors'].append("Tipo de arquivo não suportado."); results['failed'] = -1; return results

        if df.empty:
             results['errors'].append("O arquivo enviado está vazio ou não pôde ser lido."); results['failed'] = -1; return results

        df.columns = [col.strip().lower() for col in df.columns]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            results['errors'].append(f"Colunas obrigatórias ausentes: {', '.join(missing_cols)}"); results['failed'] = -1; return results

        for index, row in df.iterrows():
            # Tratar NaNs que podem vir do Excel ou CSVs malformados
            nome = str(row.get('nome', '')).strip()
            tipo = str(row.get('tipo', '')).strip()
            line_num = index + 2

            if not nome or not tipo:
                results['failed'] += 1; results['errors'].append(f"Linha {line_num}: Nome ou Tipo vazio."); continue

            success_db, message_db = gerenciador_bd.adicionar_cliente(nome, tipo)
            if success_db: results['success'] += 1
            else: results['failed'] += 1; results['errors'].append(f"Linha {line_num} ({nome}): {message_db}")

    except pd.errors.EmptyDataError: results['errors'].append("Arquivo vazio."); results['failed'] = -1
    except Exception as e:
        st.error(f"Erro inesperado ao processar arquivo de clientes: {e}")
        results['errors'].append(f"Erro geral: {e}"); results['failed'] = -1

    return results

def processar_arquivo_associacoes(gerenciador_bd: GerenciadorBD, uploaded_file):
    """Lê, valida e tenta associar colaboradores a clientes de CSV/XLSX."""
    results = {'success': 0, 'failed': 0, 'errors': []}
    required_cols = config.ASSOC_UPLOAD_REQUIRED_COLS

    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(('.xls', '.xlsx')):
            excel_data = BytesIO(uploaded_file.getvalue())
            df = pd.read_excel(excel_data, engine='openpyxl')
        else:
            results['errors'].append("Tipo de arquivo não suportado."); results['failed'] = -1; return results

        if df.empty:
             results['errors'].append("O arquivo enviado está vazio ou não pôde ser lido."); results['failed'] = -1; return results

        df.columns = [col.strip().lower() for col in df.columns]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            results['errors'].append(f"Colunas ausentes: {', '.join(missing_cols)}"); results['failed'] = -1; return results

        user_cache = {}
        client_cache = {}

        for index, row in df.iterrows():
            username = str(row.get('colaborador_username', '')).strip()
            cliente_nome = str(row.get('cliente_nome', '')).strip()
            line_num = index + 2

            if not username or not cliente_nome:
                results['failed'] += 1; results['errors'].append(f"Linha {line_num}: Colaborador ou Cliente vazio."); continue

            if username not in user_cache:
                user_cache[username] = gerenciador_bd.buscar_usuario(username) is not None
            if not user_cache[username]:
                results['failed'] += 1; results['errors'].append(f"Linha {line_num}: Colaborador '{username}' não encontrado."); continue

            if cliente_nome not in client_cache:
                cliente_data = gerenciador_bd.buscar_cliente_por_nome(cliente_nome)
                client_cache[cliente_nome] = cliente_data['id'] if cliente_data else None
            cliente_id = client_cache[cliente_nome]
            if cliente_id is None:
                 results['failed'] += 1; results['errors'].append(f"Linha {line_num}: Cliente '{cliente_nome}' não encontrado."); continue

            success_db, message_db = gerenciador_bd.associar_colaborador_cliente(username, cliente_id)
            if success_db: results['success'] += 1
            else: results['failed'] += 1; results['errors'].append(f"Linha {line_num} ({username}<->{cliente_nome}): {message_db}")

    except pd.errors.EmptyDataError: results['errors'].append("Arquivo vazio."); results['failed'] = -1
    except Exception as e:
        st.error(f"Erro inesperado ao processar arquivo de associações: {e}")
        results['errors'].append(f"Erro geral: {e}"); results['failed'] = -1

    return results

# --- END OF FILE file_processors.py ---