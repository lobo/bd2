DECLARE
  NDAYS NUMBER;
BEGIN
  NDAYS := 1000;
  
  SCORING_PKG.CALCULATE_SELLER_SCORING(
    NDAYS => NDAYS
  );
--rollback; 
END;
/
CREATE INDEX REVIEW_USER_MODIFIED_IDX
ON REVIEW ("USER_ID","ROLE", "STATUS", "MODIFIED")
tablespace team2_indexes;
/

drop index REVIEW_USER_MODIFIED_IDX;
/
CREATE INDEX REVIEW_USER_CREATED_IDX
ON REVIEW ("USER_ID","CREATED")
tablespace team2_indexes;
/
drop index REVIEW_USER_CREATED_IDX;
/



DROP TABLE SCORE;
/

CREATE TABLE "BDII_TEAM3"."SCORE" 
(	"USER_ID" NUMBER(10,0), 
	"LAST_WEEK_POSITIVE" NUMBER(10,0), 
	"LAST_WEEK_NEUTRAL" NUMBER(10,0), 
	"LAST_WEEK_NEGATIVE" NUMBER(10,0), 
	"LAST_MONTH_POSITIVE" NUMBER(10,0), 
	"LAST_MONTH_NEUTRAL" NUMBER(10,0), 
	"LAST_MONTH_NEGATIVE" NUMBER(10,0), 
	"LAST_6MONTH_POSITIVE" NUMBER(10,0), 
	"LAST_6MONTH_NEUTRAL" NUMBER(10,0), 
	"LAST_6MONTH_NEGATIVE" NUMBER(10,0), 
	"SCORE" NUMBER(10,2), 
	"STATUS" CHAR(10 BYTE), 
	"CREATED" TIMESTAMP (6), 
	"MODIFIED" TIMESTAMP (6)
) SEGMENT CREATION IMMEDIATE 
PCTFREE 10 PCTUSED 10 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
STORAGE(INITIAL 10240 NEXT 10240 MINEXTENTS 1 MAXEXTENTS 121
PCTINCREASE 5 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
TABLESPACE "BDII_TEAM3_DATA" ;
/



SELECT DISTINCT USER_ID 
   FROM REVIEW 
   WHERE ROLE = 'SELLER' 
   AND STATUS = 'PUBLISHED' 
   AND MODIFIED >= ( SYSDATE - 1000 ) 
   UNION
   SELECT USER_ID
   FROM SCORE
   WHERE MODIFIED < ( SYSDATE - 7 )
/   

SELECT CREATED, SCORE
          FROM REVIEW
          WHERE USER_ID = 123
          AND CREATED > SYSDATE - 180
/

AlTER TABLE SCORE ADD CONSTRAINT "SCORE_EUSER_FK" FOREIGN KEY ("USER_ID")
	  REFERENCES "BDII_TEAM2"."EUSER" ("ID") ENABLE;
/

AlTER TABLE EUSER 
MODIFY CREATED date;
/
AlTER TABLE EUSER 
MODIFY MODIFIED date;
/


AlTER TABLE SCORE 
MODIFY CREATED date;
/
AlTER TABLE SCORE 
MODIFY MODIFIED date;
/
AlTER TABLE Score 
MODIFY Status char(10);
/
AlTER TABLE Score 
MODIFY Status number(3, 2);
/

AlTER TABLE REVIEW 
MODIFY COMMENTS varchar2(200);
/

AlTER TABLE Sale 
MODIFY CREATED date;
/

AlTER TABLE Sale 
MODIFY MODIFIED date;
/

AlTER TABLE Review 
MODIFY CREATED date;
/

AlTER TABLE Review 
MODIFY MODIFIED date;
/

ALTER TABLE EUSER PCTFREE 0;
ALTER TABLE REVIEW PCTFREE 0;
ALTER TABLE SALE PCTFREE 0;
/

drop index REVIEW_STATUS_IDX;
/

analyze table EMPLE compute statistics;
select TABLE_NAME, NUM_ROWS, BLOCKS, EMPTY_BLOCKS, AVG_ROW_LEN
from USER_TABLES
where TABLE_NAME='EMPLE'
/

create or replace PACKAGE BODY scoring_pkg AS  -- body

   -- Cursor de vendedores cuyas calificaciones sufrieron modificaciones
   -- o cuya reputacion hay que actualizar porque es "vieja" (de mas de 1 semana)
   CURSOR SELLER_CUR( pInterval NUMBER ) IS 
   SELECT DISTINCT USER_ID 
   FROM REVIEW 
   WHERE ROLE = 'SELLER' 
   AND STATUS = 'PUBLISHED' 
   AND MODIFIED >= ( SYSDATE - pInterval ) 
   UNION
   SELECT USER_ID
   FROM SCORE
   WHERE MODIFIED < ( SYSDATE - 7 )
   ;

   CURSOR LAST_MONTHS_REVIEWS(pUserId NUMBER ) IS
          SELECT CREATED, SCORE
          FROM REVIEW
          WHERE USER_ID = pUserId
          AND CREATED > SYSDATE - 180;

    
   -- Procedimiento que actualiza la reputacion de un vendedor 
   PROCEDURE update_seller_scoring( pUserId NUMBER, 
	  p7Positive NUMBER, p7Negative NUMBER, p7Neutral NUMBER,
	  p30Positive NUMBER, p30Negative NUMBER, p30Neutral NUMBER,
	  p180Positive NUMBER, p180Negative NUMBER, p180Neutral NUMBER,
	  pScoreTotal NUMBER, pCountTotal NUMBER
	  ) AS
	  vRATIO NUMBER(3,2):=0;
   BEGIN
		
		IF pCountTotal <=0 THEN 
			vRATIO:=0;
		ELSE 
			vRATIO:=round(pScoreTotal/pCountTotal,2) ;
		END IF;
   
	   --Intento la actualización
       UPDATE SCORE 
	   SET  
	   	LAST_WEEK_POSITIVE = p7Positive,
		LAST_WEEK_NEUTRAL = p7Neutral,
		LAST_WEEK_NEGATIVE  = p7Negative,
		LAST_MONTH_POSITIVE = p30Positive,
		LAST_MONTH_NEUTRAL = p30Neutral,
		LAST_MONTH_NEGATIVE = p30Negative,
		LAST_6MONTH_POSITIVE = p180Positive,
		LAST_6MONTH_NEUTRAL = p180Neutral,
		LAST_6MONTH_NEGATIVE = p180Negative,
		SCORE = vRATIO,
		MODIFIED = sysdate
	   WHERE USER_ID = pUserId;
	   -- Si no existe, inserto el registro
	   IF SQL%NOTFOUND then
         INSERT INTO SCORE ( USER_ID, 
				LAST_WEEK_POSITIVE, LAST_WEEK_NEUTRAL, LAST_WEEK_NEGATIVE,
				LAST_MONTH_POSITIVE, LAST_MONTH_NEUTRAL, LAST_MONTH_NEGATIVE,
				LAST_6MONTH_POSITIVE, LAST_6MONTH_NEUTRAL, LAST_6MONTH_NEGATIVE,
				SCORE, STATUS, CREATED, MODIFIED ) 
         VALUES ( pUserId,
				p7Positive, p7Neutral, p7Negative,
				p30Positive, p30Neutral, p30Negative,
				p180Positive, p180Neutral, p180Negative,
				vRATIO, 'ACTIVE', SYSDATE, SYSDATE
				);
       END IF;
	   COMMIT;
   END update_seller_scoring;
   
   -- Procedimiento que calcula el puntaje de los vendedores
   PROCEDURE calculate_seller_scoring( ndays NUMBER )  AS
      v7Positive NUMBER(10) := 0;
	  v7Negative NUMBER(10) := 0;
	  v7Neutral NUMBER(10) := 0;
	  v30Positive NUMBER(10) := 0;
	  v30Negative NUMBER(10) := 0;
	  v30Neutral NUMBER(10) := 0;
	  v180Positive NUMBER(10) := 0;
	  v180Negative NUMBER(10) := 0;
	  v180Neutral NUMBER(10) := 0;
	  vScoreTotal NUMBER(10);
	  vCountTotal NUMBER(10);
   BEGIN
      
	  -- Por cada seller,
	  FOR SELLER_REC IN SELLER_CUR(ndays) LOOP
		-- Buscar el puntaje recibido en las ventas de la ultima semana
            FOR REVIEW_REC IN LAST_MONTHS_REVIEWS(SELLER_REC.USER_ID) LOOP
	      IF REVIEW_REC.SCORE = 'POSITIVE'  THEN 
				v180Positive := v180Positive + 1;
				IF REVIEW_REC.CREATED >  SYSDATE - 30  THEN
					v30Positive := v30Positive + 1;
                    IF REVIEW_REC.CREATED >  SYSDATE - 7  THEN
                        v7Positive := v7Positive + 1;
                    END IF;
				END IF;
                
            ELSIF REVIEW_REC.SCORE = 'NEUTRAL'  THEN
				v180Neutral := v180Neutral + 1;       
				IF REVIEW_REC.CREATED >  SYSDATE - 30  THEN
					v30Neutral := v30Neutral + 1;
                    IF REVIEW_REC.CREATED >  SYSDATE - 7  THEN
                        v7Neutral := v7Neutral + 1;
                    END IF;
				END IF;    
            
			ELSIF REVIEW_REC.SCORE = 'NEGATIVE'  THEN 
				v180Negative := v180Negative + 1;
				IF REVIEW_REC.CREATED >  SYSDATE - 30  THEN
					v30Negative := v30Negative + 1;
                    IF REVIEW_REC.CREATED >  SYSDATE - 7  THEN
                        v7Negative := v7Negative + 1;
                    END IF;
				END IF;
			END IF;
		END LOOP;

                
        -- Buscar el puntaje recibido en las ventas totales	
		SELECT  nvl(sum(CASE SCORE WHEN 'POSITIVE' THEN 1 WHEN 'NEGATIVE' THEN -1 ELSE 0 END),0), COUNT(1)
		INTO vScoreTotal, vCountTotal
		FROM REVIEW
		WHERE USER_ID = SELLER_REC.USER_ID;
		-- Insertar el puntaje actualizado
		update_seller_scoring( SELLER_REC.USER_ID, 
						v7Positive, v7Negative, v7Neutral, 
						v30Positive, v30Negative, v30Neutral, 
						v180Positive, v180Negative, v180Neutral, 
						vScoreTotal, vCountTotal
						); 
	  END LOOP;
	  
   END calculate_seller_scoring;
END scoring_pkg;
                
