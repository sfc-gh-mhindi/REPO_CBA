WITH Transformer_6 AS (
	SELECT
		CONV_M,
		PROS_KEY_I
	FROM
		{{ ref("util_pros_isac__loadgdwproskeyseq") }}
)

SELECT * FROM Transformer_6