DELIMITER //

CREATE PROCEDURE ETL_STG_EMPLOYEES()
BEGIN
  -- VARIABLES GENERALES
  DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT 'ETL_STG_EMPLOYEES';
  DECLARE V_FEC_INICIO DATE;
  DECLARE V_FEC_FIN DATE;
  DECLARE V_COMENTARIO VARCHAR(255);
  DECLARE V_CANT_REG INT DEFAULT 0;
  DECLARE V_CORRECTO CHAR(1) DEFAULT 'N'; -- INDICADOR DE QUE SI EL PROCESO ESTA CORRECTO O NO
  
  -- VARIABLES DEL PROCESO
  DECLARE v_total_diferencias INT DEFAULT 0;
  
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    SET V_FEC_FIN = SYSDATE();
    SET V_COMENTARIO = CONCAT('ERROR AL ACTUALIZAR ', V_NOMBRE_PROCESO, ': ', SQLCODE(), ' ', SQLERRM());
    SET V_CORRECTO = 'N';
    CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
  END;

  SET V_FEC_INICIO = SYSDATE();

  -- CODIGO DEL PROCESO
  TRUNCATE TABLE STG_EMPLOYEES;
  
  INSERT INTO STG_EMPLOYEES (EMPLOYEEID, LASTNAME, FIRSTNAME, TITLE, TITLEOFCOURTESY, BIRTHDATE, HIREDATE, ADDRESS, CITY, REGION, POSTALCODE, COUNTRY, HOMEPHONE, EXTENSION, NOTES, REPORTSTO)
  SELECT EMPLOYEEID, LASTNAME, FIRSTNAME, TITLE, TITLEOFCOURTESY, BIRTHDATE, HIREDATE, ADDRESS, CITY, REGION, POSTALCODE, COUNTRY, HOMEPHONE, EXTENSION, NOTES, REPORTSTO FROM ORDERS.EMPLOYEES;
    
  SET V_CANT_REG = ROW_COUNT();
  COMMIT;
  
  -- FIN CODIGO DEL PROCESO
  SET V_FEC_FIN = SYSDATE();
  SET V_COMENTARIO = CONCAT('EL PROCESO ', V_NOMBRE_PROCESO, ' CULMINO SATISFACTORIAMENTE');
  SET V_CORRECTO = 'S';

  CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
  COMMIT;
END //

DELIMITER ;

