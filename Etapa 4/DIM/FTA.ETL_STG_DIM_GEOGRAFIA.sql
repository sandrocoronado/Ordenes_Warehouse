DELIMITER $$
CREATE PROCEDURE ETL_STG_DIM_GEOGRAFIA()
BEGIN
-- VARIABLES GENERALES
DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT 'ETL_STG_DIM_GEOGRAFIA';
DECLARE V_FEC_INICIO DATETIME;
DECLARE V_FEC_FIN DATETIME;
DECLARE V_COMENTARIO VARCHAR(255);
DECLARE V_CANT_REG INT DEFAULT 0;
DECLARE V_CORRECTO CHAR(1) DEFAULT 'N';

-- VARIABLES DEL PROCESO
DECLARE v_total_diferencias INT DEFAULT 0;

SET V_FEC_INICIO = NOW();

-- CODIGO DEL PROCESO

TRUNCATE TABLE STG_DIM_GEOGRAFIA;

INSERT INTO STG_DIM_GEOGRAFIA (IDW_GEOGRAFIA, ADRESS, CITY, REGION, POSTALCODE, COUNTRY, STAR_DATE, END_DATE, CURRENT_FLAG, LOGIN_USER_DW)
SELECT
COALESCE((SELECT IDW_GEOGRAFIA FROM STAR_O.DIM_GEOGRAFIA WHERE IDW_GEOGRAFIA = A.ORDERID), A.ORDERID) AS IDW_GEOGRAFIA,
A.SHIPADDRESS,
A.SHIPCITY,
A.SHIPREGION,
A.SHIPPOSTALCODE,
A.SHIPCOUNTRY,
NOW() AS STAR_DATE,
NOW() AS END_DATE,
1 AS CURRENT_FLAG,
SYSTEM_USER() AS LOGIN_USER_DW
FROM STG_ORDERS_ A;

SET V_CANT_REG = ROW_COUNT();
COMMIT;

-- FIN CODIGO DEL PROCESO

SET V_FEC_FIN = NOW();
SET V_COMENTARIO = CONCAT('EL PROCESO ', V_NOMBRE_PROCESO, ' CULMINO SATISFACTORIAMENTE');
SET V_CORRECTO = 'S';

CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
COMMIT;

-- MANEJO DE ERRORES
BEGIN
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
SET V_FEC_FIN = NOW();
SET V_COMENTARIO = CONCAT('ERROR AL ACTUALIZAR ', V_NOMBRE_PROCESO, ' ', SQLCODE, ' ', SQLERRM);
CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
ROLLBACK;
END;
END;
END $$
DELIMITER ;
