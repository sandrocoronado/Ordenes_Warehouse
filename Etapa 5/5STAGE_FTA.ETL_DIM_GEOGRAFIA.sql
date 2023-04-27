DELIMITER //
CREATE PROCEDURE ETL_DIM_GEOGRAFIA()
BEGIN
  -- VARIABLES GENERALES
  DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT ETL_DIM_GEOGRAFIA();
  DECLARE  V_FEC_INICIO DATE;
  DECLARE V_FEC_FIN DATE;
  DECLARE  V_COMENTARIO VARCHAR(255);
  DECLARE  V_CORRECTO CHAR(1) DEFAULT 'N';
   DECLARE  V_TOTAL_DIFERENCIAS INT DEFAULT 0; -- INDICADOR DE QUE SI EL PROCESO ESTA CORRECTO O NO
    DECLARE v_total_diferencias INT DEFAULT 0;
  END;

  SET v_fec_inicio = NOW();

  -- CODIGO DEL PROCESO
  DECLARE 
    CURSOR c_registros IS
      SELECT IDW_GEOGRAFIA, ADRESS, CITY, REGION, POSTALCODE, COUNTRY
      FROM STG_DIM_GEOGRAFIA
      WHERE IDW_GEOGRAFIA != '-1'
      AND (IDW_GEOGRAFIA, ADRESS, CITY, REGION, POSTALCODE, COUNTRY) NOT IN (
        SELECT IDW_GEOGRAFIA, ADRESS, CITY, REGION, POSTALCODE, COUNTRY
        FROM STAR_O.DIM_GEOGRAFIA
      );
    v_registro c_registros%ROWTYPE;
  END DECLARE;

  FOR v_registro IN c_registros DO
    UPDATE STAR_O.DIM_GEOGRAFIA
    SET ADRESS = v_registro.ADRESS,
      CITY = v_registro.CITY,
      REGION = v_registro.REGION,
      POSTALCODE = v_registro.POSTALCODE,
      COUNTRY = v_registro.COUNTRY
    WHERE IDW_GEOGRAFIA = v_registro.IDW_GEOGRAFIA;
    SET v_total_diferencias := v_total_diferencias + 1;
  END FOR;

  INSERT INTO STAR_O.DIM_GEOGRAFIA (IDW_GEOGRAFIA, ADRESS, CITY, REGION, POSTALCODE, COUNTRY, STAR_DATE, END_DATE, CURRENT_FLAG, LOGIN_USER_DW)
  SELECT IDW_GEOGRAFIA, ADRESS, CITY, REGION, POSTALCODE, COUNTRY, NOW(), NOW(), 1, LOGIN_USER_DW
  FROM STG_DIM_GEOGRAFIA
  WHERE IDW_GEOGRAFIA = '-1';
  SET v_cant_reg := ROW_COUNT();

  COMMIT;

  -- FIN CODIGO DEL PROCESO
  SET v_fec_fin := NOW();
  SET v_comentario := CONCAT('EL PROCESO ', v_nombre_proceso, ' ESTA OK ACTUALIZADOS:', v_total_diferencias, ' NUEVOS:', v_cant_reg);
  SET v_correcto := 'S';

  CALL P_Insertar_Info_Proc(v_nombre_proceso, v_fec_inicio, v_fec_fin, v_comentario, v_cant_reg, v_correcto);
  COMMIT;

  -- Manejo de excepciones
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET v_fec_fin := NOW();
    SET v_comentario := CONCAT('ERROR AL ACTUALIZAR ', v_nombre_proceso, ' ', SQLCODE(), ' ', SQLERRM());
    CALL P_Insertar_Info_Proc(v_nombre_proceso, v_fec_inicio, v_fec_fin, v_comentario, v_cant_reg, v_correcto);
    COMMIT;
  END;

END;

