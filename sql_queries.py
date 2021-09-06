# Comandos de criação das tabelas
create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS usuarios (
        user_id VARCHAR(255) NOT NULL UNIQUE,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        genero VARCHAR(1),
        nivel VARCHAR(100),
        PRIMARY KEY (user_id, nivel)
        )
    """,
    """
    CREATE TABLE IF NOT EXISTS artistas (
        artist_id VARCHAR(255) UNIQUE NOT NULL,
        nome VARCHAR(100) NOT NULL,
        localizacao VARCHAR(255) UNIQUE,
        latitude REAL,
        longitude REAL,
        PRIMARY KEY (artist_id, localizacao)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS musicas (
        song_id VARCHAR(255) NOT NULL UNIQUE,
        titulo VARCHAR(255) NOT NULL,
        artist_id VARCHAR(255) NOT NULL,
        ano INTEGER,
        duracao REAL NOT NULL,
        PRIMARY KEY (song_id),
        FOREIGN KEY (artist_id)
            REFERENCES artistas (artist_id)
            ON UPDATE CASCADE ON DELETE CASCADE
        )
    """, 
    """
    CREATE TABLE IF NOT EXISTS tempo (
        start_time BIGINT PRIMARY KEY,
        hora INTEGER,
        dia INTEGER,
        semana INTEGER,
        mes INTEGER,
        ano INTEGER,
        dia_semana INTEGER 
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL NOT NULL PRIMARY KEY, 
        start_time BIGINT NOT NULL, 
        user_id VARCHAR(255) NOT NULL, 
        nivel VARCHAR(100) NOT NULL, 
        song_id VARCHAR(255), 
        artist_id VARCHAR(255), 
        session_id INTEGER NOT NULL, 
        localizacao VARCHAR(255), 
        user_agent VARCHAR(255)
    )
    """
    ]

# Comandos de remoção das tabelas
drop_table_queries = [
    """drop table if exists songplays""",
    """drop table if exists usuarios""",
    """drop table if exists musicas""",
    """drop table if exists artistas""", 
    """drop table if exists tempo"""
    ]

# insere registros na tabela de músicas
song_table_insert ="""INSERT INTO musicas (song_id, titulo, artist_id, ano, duracao) VALUES (%s, %s, %s, %s, %s );"""

# insere registros na tabela de artistas
artist_table_insert ="""INSERT INTO artistas (artist_id, nome, localizacao, latitude, longitude) VALUES (%s, %s, %s, %s, %s );"""

# insere registros na tabela de tempo
time_table_insert ="""INSERT INTO tempo (start_time, hora, dia, semana, mes, ano, dia_semana) VALUES (%s, %s, %s, %s, %s, %s, %s );"""

# insere registros na tabela de usuários
user_table_insert ="""INSERT INTO usuarios (user_id, first_name, last_name, genero, nivel) VALUES (%s, %s, %s, %s, %s );"""

# seleciona uma música baseada no nome do artista, titulo da música e sua duração
song_select = """SELECT a.nome, m.titulo, m.duracao FROM artistas a JOIN musicas m ON m.artist_id = a.artist_id WHERE a.nome = (%s) AND m.titulo = (%s) AND m.duracao = (%s);"""

# insere registros na tabela de songplay
songplay_table_insert ="""INSERT INTO songplays (start_time, user_id, nivel, song_id, artist_id, session_id, localizacao, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s ) RETURNING songplay_id;"""
