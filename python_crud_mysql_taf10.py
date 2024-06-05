# -*- coding: utf-8 -*-

# programa: CRUD - MYSQL
# notas: Esta aplicação pretende permitir, após configurados os dados de
#        ligação a uma base de dados MySQL, fazer a gestão das tabelas e
#        campos de dados da cada tabela e ainda gerir os respectivos registos.
#        O objectivo é demonstrar a apreensão dos conceitos e 
#        não uma aplicação 100% funcional e completa.
# versão: 0.2
# curso: PYTHON AVANÇADO - 24_PP01_BC_VA
# exercício: TAREFA 10 (Versão 0.2)
# autor: FERNANDO COELHO
# data: 03-06-2024

import pandas as pd
import mysql.connector
from mysql.connector import Error
import os

def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='testepython',
            user='python_user',
            password='python_1234'
        )
        if connection.is_connected():
            print()
            print("----------------------------------")
            print("Ligação à base de dados efectuada.")
            print("----------------------------------")
            return connection
    except Error as e:
        print()
        print(f"Erro: {e}")
        return None

def table_exists(connection, table_name):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    resultado = cursor.fetchone()
    return resultado is not None

def create_table(connection, table_name, schema):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
        connection.commit()
        print()
        print(f"Tabela {table_name} criada com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")

def list_tables(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except Error as e:
        print()
        print(f"Erro: {e}")
        return None

def rename_table(connection, old_name, new_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"RENAME TABLE {old_name} TO {new_name}")
        connection.commit()
        print()
        print(f"Nome da tabela {old_name} alterado para {new_name} com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")

def drop_table(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        connection.commit()
        print()
        print(f"Tabela {table_name} eliminada com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")

def list_columns(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        return [column[0] for column in columns]
    except Error as e:
        print()
        print(f"Erro: {e}")
        return None

def add_column(connection, table_name, column_definition):
    try:
        cursor = connection.cursor()
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")
        connection.commit()
        print()
        print(f"Coluna adicionada com sucesso à tabela {table_name}")
    except Error as e:
        print()
        print(f"Erro: {e}")

def modify_column(connection, table_name, old_column_name, new_definition):
    try:
        cursor = connection.cursor()
        cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_definition}")
        connection.commit()
        print()
        print(f"Nome da coluna {old_column_name} da tabela {table_name} alterado com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")

def drop_column(connection, table_name, column_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
        connection.commit()
        print()
        print(f"A coluna {column_name} da tabela {table_name} eliminada com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")

def insert_record(connection, table_name, record):
    try:
        cursor = connection.cursor()
        placeholders = ', '.join(['%s'] * len(record))
        columns = ', '.join(record.keys())
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, list(record.values()))
        connection.commit()
        print()
        print(f"Novo registo adicionado à tabela {table_name} com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")

def retrieve_records(connection, table_name, columns="*", condition=None):
    try:
        cursor = connection.cursor(dictionary=True)
        sql = f"SELECT {columns} FROM {table_name}"
        if condition:
            sql += f" WHERE {condition}"
        cursor.execute(sql)
        records = cursor.fetchall()
        return records
    except Error as e:
        print()
        print(f"Erro: {e}")
        return None

def update_record(connection, table_name, updates, condition):
    try:
        cursor = connection.cursor()
        update_clause = ', '.join([f"{key} = %s" for key in updates.keys()])
        sql = f"UPDATE {table_name} SET {update_clause} WHERE {condition}"
        cursor.execute(sql, list(updates.values()))
        connection.commit()
        print()
        print(f"O registo alterado da tabela {table_name} foi actualizado com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")

def delete_record(connection, table_name, condition):
    try:
        cursor = connection.cursor()
        sql = f"DELETE FROM {table_name} WHERE {condition}"
        cursor.execute(sql)
        connection.commit()
        print()
        print(f"O registo indicado da tabela {table_name} foi eliminado com sucesso.")
    except Error as e:
        print()
        print(f"Erro: {e}")
                      
def backup_bd():
    backup_path = input("Introduza o nome do ficheiro de backup da bd: ")
    comando = f"mysqldump -u python_user -ppython_1234 testepython > {backup_path}"
    erro = os.system(comando)
    print()
    if erro:
        print()
        print("Houve um erro ao criar o backup.")
        print("Erro: ", erro)
    else:
        print()
        print(f"Backup da BD realizado com sucesso! Em {backup_path}")

def restore_bd():
    backup_path = input("Introduza o nome do ficheiro de backup para restaurar: ") 
    comando = f"mysql -u python_user -ppython_1234 testepython < {backup_path}"
    erro = os.system(comando)
    print()
    if erro:
        print()
        print("Houve um erro ao restaurar o backup.")
        print("Erro: ", erro)
    else:
        print()
        print(f"Restauro da BD {backup_path} realizado com sucesso!")


# MAIN

if __name__ == "__main__":
    connection = connect_to_db()
    if connection:

        while True:
            print()
            print("MENU")
            print()
            print("Tabelas")
            print("-------")
            print("a. Listar tabelas")
            print("b. Criar tabela")
            print("c. Mudar nome à tabela")
            print("d. Apagar tabela")
            print()
            print("Colunas")
            print("-------")
            print("e. Listar colunas")
            print("f. Adicionar coluna")
            print("g. Mudar nome à coluna")
            print("h. Eliminar coluna")
            print()
            print("Registos")
            print("--------")
            print("i. Listar dados")
            print("j. Adicionar dados")
            print("k. Alterar dados")
            print("l. Eliminar dados")
            print()
            print("Cópia de segurança")
            print("------------------")
            print("m. Efectuar cópia de segurança")
            print("n. Restaurar cópia de segurança")
            print()
            print("x. Encerrar programa")
            print()

            op = input("CRUD OP > ")

            if op.lower() == "a":
                tabelas = list_tables(connection)
                print()
                print("TABELAS EXISTENTES NA BASE DE DADOS")
                print()
                for tabela in tabelas:
                    print(tabela)
            elif op.lower() == "b":
                print()
                table_name = input("Introduza o nome da nova tabela: ")
                if table_name == "":
                    print()
                    print("O nome da tabela não pode ser vazio.")
                else: 
                    print()
                    print("Por favor construa o esquema da tabela")
                    print("Exemplo: ")
                    print("id INT AUTO_INCREMENT PRIMARY KEY, nome VARCHAR(255), idade INT")
                    print("O exemplo acima iria adicionar três colunas, id como chave principal e ")
                    print("que incrementa automaticamente, nome do tipo alfanumérico com 255 caracteres. ")
                    print("E, idade do tipo inteiro.")
                    print("Introduza o seu esquema colocando uma vírgula entre cada campo.")
                    print("Caso não deseje criar o esquema agora faça <Enter> e será criado o seguinte esquema: ")
                    print("Esquema por defeito: ")
                    print("id INT AUTO_INCREMENT PRIMARY KEY, obs VARCHAR(255) ")
                    print()
                    schema = input("> ")
                    if schema == "":
                        schema = "id INT AUTO_INCREMENT PRIMARY KEY, obs VARCHAR(255)"
                    create_table(connection, table_name, schema)
            elif op.lower() == "c":
                print()
                print("ALTERAÇÃO DE NOME DE UMA TABELA")
                print()
                old_name = input("Nome da tabela a alterar: ")
                new_name = input("Novo nome da tabela: ")
                if old_name and new_name:
                    if table_exists(connection, old_name):
                        rename_table(connection, old_name, new_name)
                    else:
                        print()
                        print(f"A tabela {old_name} não existe na BD.")
                else:
                    print()
                    print("O nome da tabela e o novo nome não podem ser vazios!")
            elif op.lower() == "d":
                print()
                print("ELIMINAR UMA TABELA")
                print()
                table_name = input("Introduza o nome da tabela a eliminar: ")
                if table_name:
                    if table_exists(connection, table_name):
                        while True:
                            print()
                            confirma = input(f"Tem a certeza que deseja eliminar a tabela {table_name}? (s/n): ")
                            if confirma.lower() == "s":
                                drop_table(connection, table_name)
                                break
                            elif confirma.lower() == "n":
                                print()
                                print("Operação cancelada.")
                                break
                            else:
                                print()
                                print("Opção inválida. Confirme a operação com (S) para sim ou (N) para cancelar.")
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                else:
                    print()
                    print("O nome da tabela não pode ser vazio.")
            elif op.lower() == "e":
                print()
                print("LISTAGEM DAS COLUNAS DE UMA TABELA")
                print()
                table_name = input("Introduza o nome da tabela para listar as colunas: ")
                if table_name:
                    if table_exists(connection, table_name):
                        colunas = list_columns(connection, table_name)
                        print()
                        for coluna in colunas:
                            print(coluna)
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                else:
                    print()
                    print("O nome da tabela não pode ser vazio!")
            elif op.lower() == "f":
                print()
                print("ADICIONAR COLUNA A UMA TABELA")
                print()
                table_name = input("Introduza o nome da tabela para adicionar uma coluna: ")
                if table_name:
                    if table_exists(connection, table_name):
                        nome_coluna = input("Nome da nova coluna a adicionar à tabela: ")
                        print()
                        print("Seleccione o tipo de coluna a adicionar")
                        print("1. VARCHAR(255) - (tipo por defeito)")
                        print("2. INT")
                        print("3. INT AUTO_INCREMENT")
                        print("4. INT AUTO_INCREMENT PRIMARY KEY")
                        print("5. INT PRIMARY KEY")
                        tipo_dados = input("Introduza o tipo de dados para a coluna: ")
                        if table_name and nome_coluna:
                            if tipo_dados == "2":
                                column_definition = f"{nome_coluna} INT"
                            elif tipo_dados == "3":
                                column_definition = f"{nome_coluna} INT AUTO_INCREMENT"
                            elif tipo_dados == "4":
                                column_definition = f"{nome_coluna} INT AUTO_INCREMENT PRIMARY KEY"
                            elif tipo_dados == "5":
                                column_definition = f"{nome_coluna} INT PRIMARY KEY"
                            else:
                                column_definition = f"{nome_coluna} VARCHAR(255)"
                            add_column(connection, table_name, column_definition)
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                    
                else:
                    print()
                    print("O nome da tabela e da coluna não podem ser vazios!")
            elif op.lower() == "g":
                print()
                print("ALTERAR NOME A UMA COLUNA")
                print()
                table_name = input("Introduza o nome da tabela para alterar o nome a uma coluna: ")
                old_column_name = input("Introduza o nome da coluna a alterar: ")
                new_definition = input("Novo nome para atribuir à coluna: ")
                if table_name and old_column_name and new_definition:
                    if table_exists(connection, table_name):
                        modify_column(connection, table_name, old_column_name, new_definition)
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                else:
                    print()
                    print("Os dados solicitados não podem ser vazios!")
            elif op.lower() == "h":
                print()
                print("ELIMINAR UMA COLUNA A UMA TABELA")
                print()
                table_name = input("Introduza o nome da tabela a eliminar uma coluna: ")
                column_name = input("Introduza o nome da coluna a eliminar: ")
                if table_name and column_name:
                    if table_exists(connection, table_name):
                        while True:
                            print()
                            confirma = input(f"Tem a certeza que deseja eliminar a coluna {column_name} da tabela {table_name}? (s/n): ")
                            if confirma.lower() == "s":
                                drop_column(connection, table_name, column_name)
                                break
                            elif confirma.lower() == "n":
                                print()
                                print("Operação cancelada.")
                                break
                            else:
                                print()
                                print("Opção inválida. Confirme a operação com (S) para sim ou (N) para cancelar.")
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                else:
                    print()
                    print("O nome da tabela e o nome da coluna não podem ser vazios!")
            elif op.lower() == "i":
                print()
                print("REGISTOS DE UMA TABELA")
                print()
                table_name = input("Introduza o nome da tabela para listar dados: ")
                if table_name:
                    if table_exists(connection, table_name):
                        registos = retrieve_records(connection, table_name, columns="*", condition=None)
                        df = pd.DataFrame(registos)
                        df.index.name = "Index"
                        print()
                        if df.empty:
                            print(f"A tabela {table_name} está vazia.")
                        else:
                            print(df)
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                else:
                    print()
                    print("O nome da tabela não pode ser vazio!")
            elif op.lower() == "j":
                print()
                print("INSERIR REGISTO NOVO")
                print()
                table_name = input("Introduza o nome da tabela para adicionar registo: ")
                if table_name:
                    if table_exists(connection, table_name):
                        registo = {}
                        colunas = list_columns(connection, table_name)
                        print()
                        for i in range(len(colunas)):
                            campo = input(f"{colunas[i]}: ")
                            registo.update({colunas[i]:campo})
                        insert_record(connection, table_name, registo)
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                else:
                    print()
                    print("O nome da tabela não pode ser vazio!")
            elif op.lower() == "k":
                print()
                print("ALTERAR REGISTO")
                print()
                table_name = input("Introduza o nome da tabela para alterar registo: ")
                if table_name:
                    if table_exists(connection, table_name):
                        registos = retrieve_records(connection, table_name, columns="*", condition=None)
                        df = pd.DataFrame(registos)
                        df.index.name = "Index"
                        if not df.empty:
                            print()
                            print(df)
                            try:
                                new_registo = {}
                                print()
                                id = int(input("Qual o Index do registo a alterar? "))
                                print(df.loc[[id]])
                                registo = registos[id]
                                for key, value in registo.items():
                                    if isinstance(value, int) or value != value:
                                        print()
                                        print(f"{key}: {value}")
                                        try:
                                            value = int(input(f"Novo {key} (INT) ou <Enter> para não alterar: "))
                                            new_registo[key] = value
                                        except ValueError:
                                            print()
                                            print(f"Não houve alteração a {key}. Deve ser número inteiro!")
                                    elif isinstance(value, str) or value == None:
                                        print()
                                        print(f"{key}: {value}")
                                        new_value = input(f"Novo {key} (STRING) ou <Enter> para não alterar: ")
                                        if new_value == "":
                                            print()
                                            print(f"Não houve alteração a {key}.")
                                        else:
                                            value = new_value
                                            new_registo[key] = value
                                if new_registo:
                                    updates = new_registo
                                    first_key = next(iter(registo))
                                    first_value = registo[first_key]
                                    condition = f"{first_key}={first_value}"
                                    update_record(connection, table_name, updates, condition)
                            except ValueError:
                                print()
                                print("O Index tem que ser um número inteiro!")
                        else:
                            print()
                            print(f"Não há dados na tabela {table_name}.")
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
                else:
                    print()
                    print("O nome da tabela não pode ser vazio!")
            elif op.lower() == "l":
                print()
                print("ELIMINAR REGISTO")
                print()
                table_name = input("Introduza o nome da tabela para eliminar um registo: ")
                if table_name:
                    if table_exists(connection, table_name):
                        registos = retrieve_records(connection, table_name, columns="*", condition=None)
                        df = pd.DataFrame(registos)
                        df.index.name = "Index"
                        if not df.empty:
                            print()
                            print(df)
                            try:
                                print()
                                id = int(input("Qual o Index do registo a eliminar? "))
                                print()
                                print("Registo seleccionado para eliminar:")
                                print(df.loc[[id]])
                                registo = registos[id]
                                if registo:
                                    while True:
                                        print()
                                        confirma = input("Tem a certeza que deseja eliminar o registo indicado? (s/n): ")
                                        if confirma.lower() == "s":
                                            updates = registo
                                            first_key = next(iter(registo))
                                            first_value = registo[first_key]
                                            condition = f"{first_key}={first_value}"
                                            delete_record(connection, table_name, condition)
                                            break
                                        elif confirma.lower() == "n":
                                            print()
                                            print("Operação cancelada.")
                                            break
                                        else:
                                            print()
                                            print("Opção inválida. Confirme a operação com (S) para sim ou (N) para cancelar.")
                            except ValueError:
                                print()
                                print("O Index tem que ser um número inteiro!")
                        else:
                            print()
                            print(f"Não há dados na tabela {table_name}.")
                    else:
                        print()
                        print(f"A tabela {table_name} não existe na BD.")
            elif op.lower() == "m":
                print()
                print("FAZER CÓPIA DE SEGURANÇA")
                print()
                backup_bd()
            elif op.lower() == "n":
                print()
                print("RESTAURAR CÓPIA DE SEGURANÇA")
                print()
                restore_bd()
                connection.close()
                connection = connect_to_db()
            elif op.lower() == "x":
                print()
                print("Fim de programa. Obrigado! ")
                print()
                connection.close()
                break
            else:
                print()
                print("Opção Inválida! ")

