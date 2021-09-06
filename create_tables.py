import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    
    # conectando ao banco de dados ProjetoIB 
    try:
        # conectando com o banco de dados
        conn = psycopg2.connect(
            host='localhost',
            database='ProjetoIB',
            user='postgres',
            password='postgres001',
            port='5400'
        )

        # criando o cursor
        cur = conn.cursor()

        return conn, cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    
def drop_tables(cur, conn):
    
    # Executa as queries
    for query in drop_table_queries:
        cur.execute(query)

    # Realiza as mudanças
    conn.commit()


def create_tables(cur, conn):
    # Executa as queries
    for query in create_table_queries:
        cur.execute(query)

    # Realiza as mudanças
    conn.commit()


def main():
    conn, cur = create_database()
    # Deleta as tabelas
    drop_tables(cur, conn)
    # Cria as tabelas
    create_tables(cur, conn)

    # Encerra o cursor
    cur.close()
    # Encerra a conexão
    if conn is not None:
        conn.close()

if __name__ == "__main__":
    main()