-- Qual o aluno com a maior média de notas e o valor dessa média?
SELECT
	fe.Numero_Inscricao,
	fe.Nota_Media
FROM
	fato_enem fe
WHERE fe.Nota_Media = (SELECT MAX(fe2.Nota_Media) FROM fato_enem fe2)
;

-- Qual a média geral?
-- com ausentes:
SELECT 
	AVG(fe.Nota_Media) AS Media_Geral
FROM
	fato_enem fe
;

--sem ausentes:
SELECT 
	AVG(fe.Nota_Media) AS Media_Geral
FROM
	fato_enem fe
WHERE Flag_Falta = 0
;

-- Qual o % de Ausentes?
SELECT
	CAST(a.Faltas AS FLOAT)/CAST(a.Total AS FLOAT) * 100 AS Perc_Faltas
FROM
(
	SELECT 
		COUNT(CASE WHEN fe.Flag_Falta = 1 THEN 1 END) AS Faltas,
		COUNT(*) AS Total
	FROM
		fato_enem fe 
) a
;
-- Qual o número total de Inscritos?
SELECT
	COUNT(*) AS Total
FROM
	fato_enem fe
;
-- Qual a média por disciplina?
--com ausentes:
SELECT 
	AVG(fe.Nota_Natureza) AS Media_Natureza,
	AVG(fe.Nota_Humanas) AS Media_Humanas,
	AVG(fe.Nota_Linguagens) AS Media_Linguagens,
	AVG(fe.Nota_Matematica) AS Media_Matematica,
	AVG(fe.Nota_Redacao) AS Media_Redacao
FROM 
	fato_enem fe
;

-- sem ausentes:
SELECT 
	AVG(CASE WHEN fe.Presenca_Natureza != 0 THEN fe.Nota_Natureza END) AS Media_Natureza,
	AVG(CASE WHEN fe.Presenca_Humanas !=0 THEN fe.Nota_Humanas END) AS Media_Humanas,
	AVG(CASE WHEN fe.Presenca_Linguagens != 0 THEN fe.Nota_Linguagens END) AS Media_Linguagens,
	AVG(CASE WHEN fe.Presenca_Matematica != 0 THEN fe.Nota_Matematica END) AS Media_Matematica,
	AVG(CASE WHEN fe.ID_Status_Redacao != 4 THEN fe.Nota_Redacao END) AS Media_Redacao
FROM 
	fato_enem fe
;

-- Qual a média por Sexo?
-- com ausentes:
SELECT
	Sexo,
	AVG(Nota_Media) AS Nota_Media
FROM 
	fato_enem fe
GROUP BY
	Sexo
;

-- sem ausentes:
SELECT
	Sexo,
	AVG(Nota_Media) AS Nota_Media
FROM 
	fato_enem fe
WHERE
	Flag_Falta = 0
GROUP BY
	Sexo
;


-- Qual a média por Etnia?
-- com ausentes:
SELECT 
	dcr.Cor_Raca,
	AVG(fe.Nota_Media) AS Nota_Media
FROM
	fato_enem fe 
LEFT JOIN
	dim_cor_raca dcr
ON
	fe.ID_Cor_Raca = dcr.ID_Cor_Raca
GROUP BY
	dcr.Cor_Raca
;

-- sem ausentes:
SELECT 
	dcr.Cor_Raca,
	AVG(fe.Nota_Media) AS Nota_Media
FROM
	fato_enem fe 
LEFT JOIN
	dim_cor_raca dcr
ON
	fe.ID_Cor_Raca = dcr.ID_Cor_Raca
WHERE 
	fe.Flag_Falta = 0
GROUP BY
	dcr.Cor_Raca
;