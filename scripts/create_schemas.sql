
----ARQUIVO UNICO

CREATE SCHEMA IF NOT EXISTS minio.iris
WITH (location = 's3a://teste-spark/iris/');


CREATE TABLE IF NOT EXISTS minio.iris.iris_parquet (
  sepal_length DOUBLE,
  sepal_width  DOUBLE,
  petal_length DOUBLE,
  petal_width  DOUBLE,
  class        VARCHAR
)
WITH (
  external_location = 's3a://teste-spark/iris',
  format = 'PARQUET'
);


SELECT 
  sepal_length,
  class
FROM minio.iris.iris_parquet;
LIMIT 10;

--------------------------------------
---------ARQUIVO UNICO MINIO----------

CREATE SCHEMA IF NOT EXISTS minio.t1
WITH (location = 's3a://das/transform/');

CREATE TABLE IF NOT EXISTS minio.t1.mov_mds_dic_fic (
  COD_CONJ_VDI    VARCHAR,
  COD_UN_CONS_VDI INTEGER,
  QTD_IDIC_VDI    DOUBLE,
  QTD_IFIC_VDI    INTEGER,
  QTD_IDMIC_VDI   DOUBLE
)
WITH (
  external_location = 's3a://das/transform/mov_mds_dic_fic',
  format = 'PARQUET'
);


SELECT * FROM minio.t1.mov_mds_dic_fic;
LIMIT 10;


-----------------------------------------------------
----------- TODOS OS ARQUIVOS DA PASTA--------------
CREATE SCHEMA IF NOT EXISTS minio.das
WITH (location = 's3a://das/mov_mds_dic_fic');

CREATE TABLE IF NOT EXISTS minio.das.mov_mds_dic_fic(
  COD_UN_CONS_VDI               INTEGER                            
  , QTD_IDIC_VDI                DOUBLE
  , QTD_IFIC_VDI                INTEGER                           
  , QTD_IDMIC_VDI               DOUBLE
  , DTA_CMPT_VDI                TIMESTAMP  
  , COD_TIPO_ORIG_VDI           VARCHAR(2)                      
  , COD_ORIG_VDI                VARCHAR(3)                          
  , COD_TIPO_SITU_PROC_VDI      VARCHAR(2)                          
  , COD_SITU_PROC_VDI           VARCHAR(2)                           
  , COD_FORM_CONJ_VDI           VARCHAR(2)                         
  , COD_CONJ_VDI                VARCHAR(5)                          
  , COD_TEMP_MIN_VDI            VARCHAR(3)                        
  , COD_PER_APU_VDI             VARCHAR(2)                          
  , COD_CAUSA_VDI               VARCHAR(2)                         
  , COD_SUB_CAUSA_VDI           VARCHAR(2)                         
  , COD_CPU_INIC_INTER_VDI      VARCHAR(2)
  , NUM_SEQ_OPER_INIC_INTER_VDI BIGINT  
  , NUM_SEQ_GER_INIC_INTER_VDI  BIGINT                               
  , NUM_PASSO_INIC_INTER_VDI    DOUBLE                               
  , NUM_ELE_INS_TRF_VDI         BIGINT   
  , QTD_IDIC_SEM_EMRG_VDI       DOUBLE
  , QTD_IFIC_SEM_EMRG_VDI       DOUBLE
  , QTD_IDMIC_SEM_EMRG_VDI      DOUBLE
  , COD_EMP_VDI                 VARCHAR
)
 WITH (
  external_location = 's3a://das/mov_mds_dic_fic',
  format = 'PARQUET'
);



------- QUERIES
SELECT * FROM minio.das.mov_mds_dic_fic LIMIT 10;
SELECT count(1) FROM minio.das.mov_mds_dic_fic;
