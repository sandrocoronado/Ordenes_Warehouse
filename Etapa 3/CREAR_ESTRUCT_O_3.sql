/********************CREAR ESTRUCTURAS EN EL STAGE *****/

CREATE TABLE STG_CATEGORIES
AS
SELECT * FROM orders.CATEGORIES
LIMIT 1;


CREATE TABLE STG_CUSTOMERS
AS
SELECT * FROM orders.CUSTOMERS
LIMIT 1;


CREATE TABLE STG_CUSTOMERS
AS
SELECT * FROM orders.CUSTOMERS
LIMIT 1;

CREATE TABLE STG_EMPLOYEES
AS
SELECT * FROM orders.EMPLOYEES
LIMIT 1;

CREATE TABLE STG_ORDERDETAILS
AS
SELECT * FROM orders.ORDERDETAILS
LIMIT 1;

CREATE TABLE STG_ORDERS
AS
SELECT * FROM orders.ORDERS
LIMIT 1;


CREATE TABLE STG_PRODUCTS
AS
SELECT * FROM orders.PRODUCTS
LIMIT 0;

CREATE TABLE STG_SHIPPERS
AS
SELECT * FROM orders.SHIPPERS
LIMIT 0;

CREATE TABLE STG_SUPPLIERS
AS
SELECT * FROM orders.SUPPLIERS
LIMIT 0;


/******************* copias de las dimensiones y facts del star ********/

/******* si no tengo accesos a las tablas  grant ****/

CREATE TABLE STG_DIM_CUSTOMERS
AS
SELECT * FROM star_o.dim_customers
LIMIT 0;


CREATE TABLE STG_DIM_CUSTOMERS
AS
SELECT * FROM star_o.dim_customers
LIMIT 1;

CREATE TABLE STG_DIM_EMPLOYEES
AS
SELECT * FROM star_o.dim_employees
LIMIT 1;

CREATE TABLE STG_DIM_GEOGRAFIA
AS
SELECT * FROM star_o.dim_geografia
LIMIT 1;

CREATE TABLE STG_DIM_PRODUCTS
AS
SELECT * FROM star_o.dim_products
LIMIT 1;


CREATE TABLE STG_DIM_CUSTOMERS
AS
SELECT * FROM star_o.dim_customers
LIMIT 1;

CREATE TABLE STG_DIM_EMPLOYEES
AS
SELECT * FROM star_o.dim_employees
LIMIT 1;

CREATE TABLE STG_DIM_GEOGRAFIA
AS
SELECT * FROM star_o.dim_geografia
LIMIT 1;

CREATE TABLE STG_DIM_PRODUCTS
AS
SELECT * FROM star_o.dim_products
LIMIT 1;

CREATE TABLE STG_DIM_SHIPPERS
AS
SELECT * FROM star_o.dim_shippers
LIMIT 1;


/********** CREAR ESTRUCTURA DE STG ORDERS ****/

drop table STG_ORDERS_COPIA;

CREATE TABLE STG_ORDERS_COPIA
AS
SELECT * FROM orders.orders ;

CREATE TABLE STG_FACT_ORDERS
AS
SELECT * FROM star_o.fact_orders
LIMIT 1;


/*********** CREAR ESTRUCTURA STG_DIM_TIEMPO ***/

CREATE TABLE STG_DIM_TIEMPO
AS
SELECT * FROM STAR_O.DIM_TIEMPO
WHERE ROWNUM <1 ;

/*********** CREAR UNA SEQUENCIA  ************/

CREATE TABLE SEQ_IDW_CUST (
    ID INT AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE SEQ_IDW_EMPL (
    ID INT AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE SEQ_IDW_GEO (
    ID INT AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE SEQ_IDW_PROD (
    ID INT AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE SEQ_IDW_SHIP (
    ID INT AUTO_INCREMENT PRIMARY KEY
);


/**** CREAMOS LA TABLA DE LOG DE PROCESOS ****/

CREATE TABLE LOG_DE_PROCESOS (
  NOMBRE_PROCESO VARCHAR(50),
  FEC_INICIO DATE,
  FEC_FIN DATE,
  COMENTARIO VARCHAR(500),
  CANT_REG INT,
  CORRECTO CHAR(1)
);

  /*********** CREAMOS EL PROCESO DE LOG DE PROCESOS*/ 
     
  CREATE PROCEDURE P_INSERTAR_INFO_PROC(
    IN V_NOMBRE_PROCESO VARCHAR(50),
    IN V_FEC_INICIO DATE,
    IN V_FEC_FIN DATE,
    IN V_COMENTARIO VARCHAR(500),
    IN V_CANT_REG INT,
    IN V_CORRECTO CHAR(1)
)
DELIMITER //

CREATE PROCEDURE P_INSERTAR_INFO_PROC(
    IN V_NOMBRE_PROCESO VARCHAR(50),
    IN V_FEC_INICIO DATE,
    IN V_FEC_FIN DATE,
    IN V_COMENTARIO VARCHAR(500),
    IN V_CANT_REG INT,
    IN V_CORRECTO CHAR(1)
)
BEGIN
    INSERT INTO LOG_DE_PROCESOS (NOMBRE_PROCESO, FEC_INICIO, FEC_FIN, COMENTARIO, CANT_REG, CORRECTO)
    VALUES (V_NOMBRE_PROCESO, V_FEC_INICIO, V_FEC_FIN, V_COMENTARIO, V_CANT_REG, V_CORRECTO);
END//

DELIMITER ;


CREATE PROCEDURE P_INSERTAR_INFO_PROC (
    IN V_NOMBRE_PROCESO VARCHAR(50),
    IN V_FEC_INICIO DATE,
    IN V_FEC_FIN DATE,
    IN V_COMENTARIO VARCHAR(500),
    IN V_CANT_REG INT,
    IN V_CORRECTO CHAR(1)
)
CREATE PROCEDURE P_INSERTAR_INFO_PROC (
    IN V_NOMBRE_PROCESO VARCHAR(50),
    IN V_FEC_INICIO DATE,
    IN V_FEC_FIN DATE,
    IN V_COMENTARIO VARCHAR(500),
    IN V_CANT_REG INT,
    IN V_CORRECTO CHAR(1)
)
CREATE PROCEDURE P_INSERTAR_INFO_PROC (
    IN V_NOMBRE_PROCESO VARCHAR(50),
    IN V_FEC_INICIO DATE,
    IN V_FEC_FIN DATE,
    IN V_COMENTARIO VARCHAR(500),
    IN V_CANT_REG INT,
    IN V_CORRECTO CHAR(1)
)
DELIMITER $$
CREATE PROCEDURE P_INSERTAR_INFO_PROC (
    IN V_NOMBRE_PROCESO VARCHAR(50),
    IN V_FEC_INICIO DATE,
    IN V_FEC_FIN DATE,
    IN V_COMENTARIO VARCHAR(500),
    IN V_CANT_REG INT,
    IN V_CORRECTO CHAR(1)
)
BEGIN
    INSERT INTO LOG_DE_PROCESOS (
        NOMBRE_PROCESO,
        FEC_INICIO,
        FEC_FIN,
        COMENTARIO,
        CANT_REG,
        CORRECTO
    ) VALUES (
        V_NOMBRE_PROCESO,
        V_FEC_INICIO,
        V_FEC_FIN,
        V_COMENTARIO,
        V_CANT_REG,
        V_CORRECTO
    );
    COMMIT;
END $$
DELIMITER ;



