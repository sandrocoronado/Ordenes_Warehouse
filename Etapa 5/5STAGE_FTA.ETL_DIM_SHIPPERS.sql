DELIMITER //
CREATE PROCEDURE ETL_DIM_SHIPPERS ()
BEGIN
  -- VARIABLES GENERALES
  DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT 'ETL_DIM_SHIPPERS';
  DECLARE V_FEC_INICIO DATETIME;
  DECLARE V_FEC_FIN DATETIME;
  DECLARE V_COMENTARIO VARCHAR(255);
  DECLARE V_CANT_REG INT DEFAULT 0;
  DECLARE V_CORRECTO CHAR(1) DEFAULT 'N'; -- INDICADOR DE QUE SI EL PROCESO ESTA CORRECTO O NO
 
  -- VARIABLES DEL PROCESO
  DECLARE V_TOTAL_DIFERENCIAS INT DEFAULT 0;
  
  SET V_FEC_INICIO := NOW();

  -- CODIGO DEL PROCESO
  -- Loop
  BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE V_IDW_SHIPPERS VARCHAR(50);
  DECLARE V_ID_SHIPPER VARCHAR(50);
  DECLARE V_SHIPPERS VARCHAR(200);
  DECLARE V_PHONE VARCHAR(50);

  DECLARE cur1 CURSOR FOR SELECT IDW_SHIPPERS, ID_SHIPPER, SHIPPERS, PHONE
                   FROM STG_DIM_SHIPPERS
                   MINUS;
                   SELECT IDW_SHIPPERS, ID_SHIPPER, SHIPPERS, PHONE
                   FROM STAR_O.DIM_SHIPPERS;

  BEGIN
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  OPEN cur1;

  read_loop: LOOP
    FETCH cur1 INTO V_IDW_SHIPPERS, V_ID_SHIPPER, V_SHIPPERS, V_PHONE;
    IF done THEN
      LEAVE read_loop;
    END IF;
    
    IF V_IDW_SHIPPERS != '-1' THEN
      UPDATE STAR_O.DIM_SHIPPERS
      SET ID_SHIPPER = V_ID_SHIPPER, SHIPPERS = V_SHIPPERS, PHONE = V_PHONE
      WHERE IDW_SHIPPERS = V_IDW_SHIPPERS;
      SET V_TOTAL_DIFERENCIAS := V_TOTAL_DIFERENCIAS + 1;
    END IF;
  END LOOP;
  
  -- Check if there are new records
  INSERT INTO STAR_O.DIM_SHIPPERS (IDW_SHIPPERS, ID_SHIPPER, SHIPPERS, PHONE, STAR_DATE, END_DATE, CURRENT_FLAG, LOGIN_USER_DW)
  SELECT SEQ_IDW_SHIP.NEXTVAL, ID_SHIPPER, SHIPPERS, PHONE, NOW(), NOW(), 1, LOGIN_USER_DW
  FROM STG_DIM_SHIPPERS
  WHERE IDW_SHIPPERS = -1;

  SET V_CANT_REG := ROW_COUNT();
  
  -- FIN CODIGO DEL PROCESO
  SET V_FEC_FIN := NOW();
  SET V_COMENTARIO := CONCAT('EL PROCESO ', V_NOMBRE_PROCESO, ' ESTA OK ACTUALIZADOS: ', V_TOTAL_DIFERENCIAS, ' NUEVOS: ', V_CANT_REG);
  SET V_CORRECTO := 'S';

  CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);

  COMMIT;
  
  -- Handle errors
  BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    SET V_FEC_FIN := NOW();
    SET V_COMENTARIO := CONCAT('ERROR AL ACTUALIZAR ', V_NOMBRE_PROCESO, ' ', SQLCODE, ' ', SQLERRM);
	CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
	END;
  -- FIN CODIGO DEL PROCESO

SET v_fec_fin = NOW();
SET v_comentario = CONCAT('EL PROCESO ', v_nombre_proceso, ' ESTA OK ACTUALIZADOS:', V_TOTAL_DIFERENCIAS, ' NUEVOS:', V_CANT_REG);
SET v_correcto = 'S';

CALL P_Insertar_Info_Proc(v_nombre_proceso, v_fec_inicio, v_fec_fin, v_comentario, v_cant_reg, v_correcto);
COMMIT;
BEGIN
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
  ROLLBACK;
  SET v_fec_fin = NOW();
  SET v_comentario = CONCAT('ERROR AL ACTUALIZAR ', v_nombre_proceso, ' ', SQLCODE, ' ', SQLERRM);
  CALL P_Insertar_Info_Proc(v_nombre_proceso,
                            v_fec_inicio,
                            v_fec_fin,
                            v_comentario,
                            v_cant_reg,
                            v_correcto);
  COMMIT;
END;
END;
END;
END;
END;
END;

