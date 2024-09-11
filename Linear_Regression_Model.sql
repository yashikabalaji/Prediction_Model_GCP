
CREATE OR REPLACE MODEL `vlba-rsd-grp3.RSD_department.LR_predict_success_percentage`
OPTIONS(
  MODEL_TYPE='LINEAR_REG',
  INPUT_LABEL_COLS = ['success_value'],
  L2_REG=0.1,
  L1_REG=0.1,
  DATA_SPLIT_EVAL_FRACTION=0.1,
  DATA_SPLIT_METHOD="RANDOM",
  MAX_ITERATIONS = 50
) AS
SELECT * EXCEPT (PERNR)
FROM `vlba-rsd-grp3.RSD_department.employee_details_with_success_value`;