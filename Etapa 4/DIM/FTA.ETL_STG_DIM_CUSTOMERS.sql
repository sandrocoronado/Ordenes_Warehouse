DELIMITER //
CREATE PROCEDURE ETL_STG_DIM_CUSTOMERS()
BEGIN
  DECLARE V_NOMBRE_PROCESO VARCHAR(30);
  DECLARE V_FEC_INICIO DATE;
  DECLARE V_FEC_FIN DATE;
  DECLARE V_COMENTARIO VARCHAR(255);
  DECLARE V_CANT_REG INT DEFAULT 0;
  DECLARE V_CORRECTO CHAR(1) DEFAULT 'N';
  DECLARE v_total_diferencias INT DEFAULT 0;

  SET V_NOMBRE_PROCESO = 'ETL_STG_DIM_CUSTOMERS';
  SET V_FEC_INICIO = NOW();

  -- CODIGO DEL PROCESO
  TRUNCATE TABLE STG_DIM_CUSTOMERS;

  INSERT INTO STG_DIM_CUSTOMERS (
    IDW_CUSTOMERS,
    ID_CUSTOMER,
    CUSTOMER,
    CUSTOMER_CONTACT,
    CONTACTTITLE,
    ADDRESS,
    CITY,
    REGION,
    POSTALCODE,
    COUNTRY,
    PHONE,
    FAX,
    PK_SYSTEM_SOURCE,
    START_DATE,
    END_DATE,
    CURRENT_FLAG,
    LOGIN_USER_DW
  )
  SELECT 
    COALESCE(
      (
        SELECT A.IDW_CUSTOMERS
        FROM STAR_O.DIM_CUSTOMERS A
        WHERE A.ID_CUSTOMER = SCR.CUSTOMERID
      ), -1
    ) IDW_CUSTOMERS,
    SCR.CUSTOMERID,
    SCR.COMPANYNAME,
    SCR.CONTACTNAME,
    SCR.CONTACTTITLE,
    SCR.ADDRESS,
    SCR.CITY,
    SCR.REGION,
    SCR.POSTALCODE,
    SCR.COUNTRY,
    SCR.PHONE,
    SCR.FAX,
    1 PK_SYSTEM_SOURCE,
    NOW() AS START_DATE,
    NOW() AS END_DATE,
    1 CURRENT_FLAG,
    SYS_CONTEXT('USERENV', 'OS_USER') LOGIN_USER_DW
  FROM 
    STG_CUSTOMERS SCR
    INNER JOIN STAR_O.DIM_CUSTOMERS A ON SCR.CUSTOMERID = A.ID_CUSTOMER;
  
  SET V_CANT_REG = ROW_COUNT();
  COMMIT;

  -- FIN CODIGO DEL PROCESO
  SET V_FEC_FIN = NOW();
  SET V_COMENTARIO = CONCAT('EL PROCESO ', V_NOMBRE_PROCESO, ' CULMINO SATISFACTORIAMENTE ');
  SET V_CORRECTO = 'S';

  CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
  COMMIT;
 BEGIN 
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET V_FEC_FIN = NOW();
    SET V_COMENTARIO = CONCAT('ERROR AL ACTUALIZAR ', V_NOMBRE_PROCESO, ' ', SQLCODE(), ' ', SQLERRM());
    CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
    COMMIT;
  END;
   END;
   END//
DELIMITER ;
