CREATE DATABASE enem GO
USE enem GO

CREATE TABLE dim_escola (
	ID_Escola INT,
	Tipo_Escola VARCHAR(13),
	Tipo_Ensino VARCHAR(43),
	Codigo_Municipio INT,
	Nome_Municipio VARCHAR(150),
	Codigo_UF TINYINT,
	Sigla_UF CHAR(2),
	Dep_Adm VARCHAR(9),
	Localizacao VARCHAR(6),
	Situacao_Funcionamento VARCHAR(34)
)

CREATE TABLE dim_cor_raca (
	ID_Cor_Raca TINYINT,
	Cor_Raca VARCHAR(13)
)

CREATE TABLE dim_presenca (
	ID_Presenca TINYINT,
	Descricao_Presenca VARCHAR(18)
)


CREATE TABLE fato_enem(
	Numero_Inscricao CHAR(12),
	Ano SMALLINT,
	Sexo CHAR(1),
	ID_Cor_Raca TINYINT,
	ID_Escola INT,
	Nota_Natureza DECIMAL(5,1),
	Nota_Humanas DECIMAL(5,1),
	Nota_Linguagens DECIMAL(5,1),
	Nota_Matematica DECIMAL(5,1),
	Nota_Redacao DECIMAL(5,1),
	Presenca_Natureza TINYINT,
	Presenca_Humanas TINYINT,
	Presenca_Linguagens TINYINT,
	Presenca_Matematica TINYINT,
	Flag_Falta BIT,
	Nota_Media FLOAT
)

GO

