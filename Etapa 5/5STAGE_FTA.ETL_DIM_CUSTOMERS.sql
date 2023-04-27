DELIMITER //

CREATE PROCEDURE ETL_DIM_CUSTOMERS()
BEGIN
    -- VARIABLES GENERALES
    DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT 'ETL_DIM_CUSTOMERS';
    DECLARE V_FEC_INICIO DATETIME;
    DECLARE V_FEC_FIN DATETIME;
    DECLARE V_COMENTARIO VARCHAR(255);
    DECLARE V_CANT_REG INT DEFAULT 0;
    DECLARE V_CORRECTO CHAR(1) DEFAULT 'N'; -- INDICADOR DE QUE SI EL PROCESO ESTA CORRECTO O NO

    -- VARIABLES DEL PROCESO
    DECLARE v_total_diferencias INT DEFAULT 0;

    SET V_FEC_INICIO = NOW();

    -- CODIGO DEL PROCESO
    INSERT INTO STAR_O.DIM_CUSTOMERS (
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
        SYSTEM_SOURCE,
        START_DATE,
        END_DATE,
        CURRENT_FLAG,
        LOGIN_USER_DW
    )
    SELECT
        IFNULL(MAX(IDW_CUSTOMERS), 0) + 1 AS IDW_CUSTOMERS,
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
        SYSTEM_SOURCE,
        NOW() AS START_DATE,
        NOW() AS END_DATE,
        1 AS CURRENT_FLAG,
        SUBSTRING_INDEX(USER(), '@', 1) AS LOGIN_USER_DW
    FROM STG_DIM_CUSTOMERS
    WHERE IDW_CUSTOMERS = -1
    ON DUPLICATE KEY UPDATE
        ID_CUSTOMER = VALUES(ID_CUSTOMER),
        CUSTOMER = VALUES(CUSTOMER),
        CUSTOMER_CONTACT = VALUES(CUSTOMER_CONTACT),
        CONTACTTITLE = VALUES(CONTACTTITLE),
        ADDRESS = VALUES(ADDRESS),
        CITY = VALUES(CITY),
        REGION = VALUES(REGION),
        POSTALCODE = VALUES(POSTALCODE),
        COUNTRY = VALUES(COUNTRY),
        PHONE = VALUES(PHONE),
        FAX = VALUES(FAX),
        PK_SYSTEM_SOURCE = VALUES(PK_SYSTEM_SOURCE),
        SYSTEM_SOURCE = VALUES(SYSTEM_SOURCE),
        END_DATE = NOW(),
        CURRENT_FLAG = 0;

    SET v_total_diferencias = ROW_COUNT();

    -- FIN CODIGO DEL PROCESO
    SET V_FEC_FIN = NOW();
    SET V_COMENTARIO = CONCAT('EL PROCESO ', V_NOMBRE_PROCESO, ' ESTA OK ACTUALIZADOS :', v_total_diferencias, ' NUEVOS : ', V_CANT_REG);
    SET V_CORRECTO = 'S';

    CALL P_Insertar_Info_Proc(V_NOMBRE_PROCESO,
        V_FEC_INICIO,
        V_FEC_FIN,
        V_COMENTARIO,
        V_CANT_REG,
        V_CORRECTO
    );
END //

DELIMITER ;