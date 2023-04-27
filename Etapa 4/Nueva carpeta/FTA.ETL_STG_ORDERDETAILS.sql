DELIMITER //
CREATE PROCEDURE ETL_STG_ORDERDETAILS()
BEGIN
-- VARIABLES GENERALES
DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT 'STG_ORDERDETAILS';
DECLARE V_FEC_INICIO DATETIME;
DECLARE V_FEC_FIN DATETIME;
DECLARE V_COMENTARIO VARCHAR(255);
DECLARE V_CANT_REG INT DEFAULT 0;
DECLARE V_CORRECTO CHAR(1) DEFAULT 'N';

-- Manejo de excepciones
DECLARE CONTINUE HANDLER FOR EXCEPTION
BEGIN
    SET V_FEC_FIN = NOW();
    SET V_COMENTARIO = CONCAT('ERROR AL ACTUALIZAR ', V_NOMBRE_PROCESO, ' ', SQLCODE, ' ', SQLERRM);
    CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
    COMMIT;
END;

-- VARIABLES DEL PROCESO
DECLARE v_total_diferencias INT DEFAULT 0;

SET V_FEC_INICIO = NOW();

-- CODIGO DEL PROCESO
TRUNCATE TABLE STG_ORDERDETAILS;
INSERT INTO STG_ORDERDETAILS (ORDERID, PRODUCTID, UNITPRICE, QUANTITY, DISCOUNT)
SELECT ORDERID, PRODUCTID, UNITPRICE, QUANTITY, DISCOUNT FROM ORDERS.ORDERDETAILS;

SET V_CANT_REG = ROW_COUNT();
COMMIT;

-- FIN CODIGO DEL PROCESO
SET V_FEC_FIN = NOW();
SET V_COMENTARIO = CONCAT('EL PROCESO ', V_NOMBRE_PROCESO, ' CULMINO SATISFACTORIAMENTE');
SET V_CORRECTO = 'S';

CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
COMMIT;

END //
DELIMITER ;