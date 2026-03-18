--Clean the additional_information in customerdata.epsewerai_cr_inspect

CREATE OR REPLACE TRIGGER trg_cr_inspect_clean_ai
BEFORE INSERT OR UPDATE
ON customerdata.epsewerai_cr_inspect
FOR EACH ROW
BEGIN
  IF :NEW.additional_information IS NOT NULL
     AND DBMS_LOB.INSTR(TO_CLOB(:NEW.additional_information), '<') > 0
  THEN
    :NEW.additional_information :=
      REGEXP_REPLACE(
        TRIM(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  TO_CLOB(:NEW.additional_information),
                  '<(script|style)[^>]*>.*?</\1>', ' ', 1, 0, 'in'
                ),
                '<head[^>]*>.*?</head>', ' ', 1, 0, 'in'
              ),
              '<!DOCTYPE[^>]*>', ' ', 1, 0, 'in'
            ),
            '<[^>]+>', ' ', 1, 0, 'in'
          )
        ),
        '[[:space:]]+', ' '
      );
  END IF;
END;
/
ALTER TRIGGER trg_cr_inspect_clean_ai ENABLE;