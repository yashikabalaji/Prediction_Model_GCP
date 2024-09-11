
CREATE OR REPLACE MODEL `vlba-rsd-grp3.RSD_department.DNN_predict_success_score`
OPTIONS(
    MODEL_TYPE='DNN_REGRESSOR',
    ACTIVATION_FN='RELU',
    BATCH_SIZE=60,
    DROPOUT=0.1,
    EARLY_STOP=TRUE,
    HIDDEN_UNITS=[68, 8],
    INPUT_LABEL_COLS=['success_value'],
    LEARN_RATE=0.1,
    MAX_ITERATIONS=100,
    OPTIMIZER='ADAGRAD',
    DATA_SPLIT_METHOD='RANDOM',
    DATA_SPLIT_EVAL_FRACTION=0.2
)
AS SELECT * EXCEPT (PERNR) FROM `vlba-rsd-grp3.RSD_department.employee_details_with_success_value`;