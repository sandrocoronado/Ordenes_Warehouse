DELIMITER //

CREATE PROCEDURE ETL_STG_CUSTOMERS()
BEGIN
  -- VARIABLES GENERALES
  DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT 'ETL_STG_CUSTOMERS';
  DECLARE V_FEC_INICIO DATETIME;
  DECLARE V_FEC_FIN DATETIME;
  DECLARE V_COMENTARIO VARCHAR(255);
  DECLARE V_CANT_REG INT DEFAULT 0;
  DECLARE V_CORRECTO CHAR(1) DEFAULT 'N'; -- INDICADOR DE QUE SI EL PROCESO ESTA CORRECTO O NO

  -- VARIABLES DEL PROCESO
  DECLARE v_total_diferencias INT DEFAULT 0;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET V_FEC_FIN = NOW();
    SET V_COMENTARIO = CONCAT('ERROR AL ACTUALIZAR ', V_NOMBRE_PROCESO, ' ', ERRCODE(), ' ', SQLERRM());
    CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
    COMMIT;
  END;

  SET V_FEC_INICIO = NOW();

  -- CODIGO DEL PROCESO
  TRUNCATE TABLE STG_CUSTOMERS;
  INSERT INTO STG_CUSTOMERS (CUSTOMERID, COMPANYNAME, CONTACTNAME, CONTACTTITLE, ADDRESS, CITY, REGION, POSTALCODE, COUNTRY, PHONE, FAX)
  SELECT CUSTOMERID, COMPANYNAME, CONTACTNAME, CONTACTTITLE, ADDRESS, CITY, REGION, POSTALCODE, COUNTRY, PHONE, FAX FROM ORDERS.CUSTOMERS;
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






