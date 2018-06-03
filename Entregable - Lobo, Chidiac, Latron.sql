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

drop index REVIEW_STATUS_IDX;
/

analyze table EMPLE compute statistics;
select TABLE_NAME, NUM_ROWS, BLOCKS, EMPTY_BLOCKS, AVG_ROW_LEN
from USER_TABLES
where TABLE_NAME='EMPLE'
/


                