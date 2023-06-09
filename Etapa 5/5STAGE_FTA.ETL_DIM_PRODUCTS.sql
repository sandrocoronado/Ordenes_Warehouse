DELIMITER //
CREATE PROCEDURE ETL_DIM_PRODUCTS ()
BEGIN
-- VARIABLES GENERALES
DECLARE V_NOMBRE_PROCESO VARCHAR(30) DEFAULT 'ETL_DIM_PRODUCTS';
DECLARE V_FEC_INICIO DATETIME;
DECLARE V_FEC_FIN DATETIME;
DECLARE V_COMENTARIO VARCHAR(255);
DECLARE V_CANT_REG INT DEFAULT 0;
DECLARE V_CORRECTO CHAR(1) DEFAULT 'N'; -- INDICADOR DE QUE SI EL PROCESO ESTA CORRECTO O NO
-- VARIABLES DEL PROCESO
DECLARE v_total_diferencias INT DEFAULT 0;

DECLARE cur_each_row CURSOR FOR
SELECT IDW_PRODUCTS,ID_PRODUCT,PRODUCTNAME,QUANTITYPERUNIT,UNITPRICE,UNITSINSTOCK,UNITSONORDER,REORDERLEVEL
,DISCONTINUED,CATEGORYID,CATEGORYNAME,DESCRIPTION_CATEGORY,SUPPLIERID,SUPPLIER,SUPPLIERCONTACT
,SUPPLIERTITLE,ADDRES,CITY,REGION,POSTALCODE,COUNTRY,PHONE,FAX
FROM STG_DIM_PRODUCTS
WHERE IDW_PRODUCTS NOT IN (SELECT IDW_PRODUCTS FROM STAR_O.DIM_PRODUCTS);

SET V_FEC_INICIO = NOW();

-- CODIGO DEL PROCESO
OPEN cur_each_row;
each_row_loop: LOOP
FETCH cur_each_row INTO IDW_PRODUCTS,ID_PRODUCT,PRODUCTNAME,QUANTITYPERUNIT,UNITPRICE,UNITSINSTOCK,UNITSONORDER,REORDERLEVEL
,DISCONTINUED,CATEGORYID,CATEGORYNAME,DESCRIPTION_CATEGORY,SUPPLIERID,SUPPLIER,SUPPLIERCONTACT
,SUPPLIERTITLE,ADDRES,CITY,REGION,POSTALCODE,COUNTRY,PHONE,FAX;
IF (v_total_diferencias % 1000 = 0) THEN
COMMIT;
END IF;
IF (cur_each_row%NOTFOUND) THEN
LEAVE each_row_loop;
END IF;
IF IDW_PRODUCTS != -1 THEN
UPDATE STAR_O.DIM_PRODUCTS
SET ID_PRODUCT = ID_PRODUCT,
PRODUCTNAME = PRODUCTNAME,
QUANTITYPERUNIT = QUANTITYPERUNIT,
UNITPRICE = UNITPRICE,
UNITSINSTOCK = UNITSINSTOCK,
UNITSONORDER = UNITSONORDER,
REORDERLEVEL = REORDERLEVEL,
DISCONTINUED = DISCONTINUED,
CATEGORYID = CATEGORYID,
CATEGORYNAME = CATEGORYNAME,
DESCRIPTION_CATEGORY = DESCRIPTION_CATEGORY,
SUPPLIERID = SUPPLIERID,
SUPPLIER = SUPPLIER,
SUPPLIERCONTACT = SUPPLIERCONTACT,
SUPPLIERTITLE = SUPPLIERTITLE,
ADDRES = ADDRES,
CITY = CITY,
REGION = REGION,
POSTALCODE = POSTALCODE,
COUNTRY = COUNTRY,
PHONE = PHONE,
FAX = FAX
WHERE IDW_PRODUCTS = IDW_PRODUCTS;
SET v_total_diferencias := v_total_diferencias + 1;
END IF;
END LOOP each_row_loop;
CLOSE cur_each;
/************ SI NO EXISTE DIFERENCIAS SIGNIFICA QUE EXISTEN NUEVOS REGISTROS ***/
INSERT INTO STAR_O.DIM_PRODUCTS (IDW_PRODUCTS, ID_PRODUCT, PRODUCTNAME, QUANTITYPERUNIT, UNITPRICE, UNITSINSTOCK, UNITSONORDER, REORDERLEVEL, DISCONTINUED, CATEGORYID, CATEGORYNAME, DESCRIPTION_CATEGORY, SUPPLIERID, SUPPLIER, SUPPLIERTITLE, ADDRES, CITY, REGION, POSTALCODE, COUNTRY, PHONE, FAX, STAR_DATE, END_DATE, CURRENT_FLAG, LOGIN_USER_DW)
SELECT (SELECT MAX(IDW_PRODUCTS) + 1 FROM STAR_O.DIM_PRODUCTS) IDW_PRODUCTS, ID_PRODUCT, PRODUCTNAME, QUANTITYPERUNIT, UNITPRICE, UNITSINSTOCK, UNITSONORDER, REORDERLEVEL, DISCONTINUED, CATEGORYID, CATEGORYNAME, DESCRIPTION_CATEGORY, SUPPLIERID, SUPPLIER, SUPPLIERTITLE, ADDRES, CITY, REGION, POSTALCODE, COUNTRY, PHONE, FAX, NOW() STAR_DATE, NOW() END_DATE, 1 CURRENT_FLAG, USER() LOGIN_USER_DW
FROM STG_DIM_PRODUCTS
WHERE IDW_PRODUCTS = -1;

  -- FIN CODIGO DEL PROCESO
SET v_fec_fin = NOW();
SET v_comentario = CONCAT('EL PROCESO ', v_nombre_proceso, ' ESTA OK ACTUALIZADOS :', V_TOTAL_DIFERENCIAS, ' NUEVOS : ', V_CANT_REG);
SET v_correcto = 'S';
CALL P_Insertar_Info_Proc(v_nombre_proceso, v_fec_inicio, v_fec_fin, v_comentario, v_cant_reg, v_correcto);
COMMIT;

END;
