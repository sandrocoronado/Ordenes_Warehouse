
 /******* DIM GEOGRAFIA ******/
CALL ETL_STG_DIM_GEOGRAFIA();
CALL ETL_DIM_GEOGRAFIA();

SELECT * FROM STAR_O.DIM_GEOGRAFIA;

/************ DIM CUSTOMERS *******/
CALL ETL_STG_CUSTOMERS();
CALL ETL_STG_DIM_CUSTOMERS();
CALL ETL_DIM_CUSTOMERS();

SELECT * FROM STAR_O.DIM_CUSTOMERS;

/******* DIM SHIPPERS ******/
CALL ETL_STG_SHIPPERS();
CALL ETL_STG_DIM_SHIPPERS();
CALL ETL_DIM_SHIPPERS();

SELECT * FROM STAR_O.DIM_SHIPPERS;

/************ DIM PRODUCTS *******/
CALL ETL_STG_CATEGORIES();
CALL ETL_STG_SUPPLIERS();
CALL ETL_STG_PRODUCTS();
CALL ETL_STG_DIM_PRODUCTS();
CALL ETL_DIM_PRODUCTS();

SELECT * FROM STAR_O.DIM_PRODUCTS;

/************ DIM EMPLOYEES *******/
CALL ETL_STG_EMPLOYEES();
CALL ETL_STG_DIM_EMPLOYEES();
CALL ETL_DIM_EMPLOYEES();

SELECT * FROM STAR_O.DIM_EMPLOYEES;

/************ DIM TIEMPO *******/
CALL ETL_STG_DIM_TIEMPO();
CALL ETL_DIM_TIEMPO();

SELECT * FROM STAR_O.DIM_TIEMPO;


/************ CARGADOD DE LA FACT TABLE *****/

SELECT MAX(ORDERDATE), MIN(ORDERDATE) FROM ORDERS;
/*5/6/1996    4/8/1994*/

SELECT MAX(REQUIREDDATE), MIN(REQUIREDDATE) FROM ORDERS;
/*--11/7/1996   24/8/1994*/

SELECT MAX(SHIPPEDDATE), MIN(SHIPPEDDATE) FROM ORDERS;
/*--5/6/1996    10/8/1994*/

/*--19940408    19961107*/

/************ FACT ORDERS *******/
/*--7/31/1999    1/31/1998
--'19980101','19990731'*/

CALL ETL_STG_ORDERDETAILS;

CALL ETL_STG_FACT_ORDERS('19940408','19961107');

CALL ETL_FACT_ORDERS ('19940408','19961107');
-- REVISAR DATOS ***/ 

/******** REVISAR LA LOG DE PROCESOS ****/  
SELECT * FROM LOG_DE_PROCESOS
ORDER BY FEC_FIN DESC;

