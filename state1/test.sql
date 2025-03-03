CREATE TABLE tb_experiment_vs_control (
    dt DATE,
    day VARCHAR(10),  -- 用 VARCHAR(10) 代替 TEXT
    variation VARCHAR(255),  -- 用 VARCHAR(255) 代替 TEXT
    control_rate DOUBLE,  -- 使用 DOUBLE 而非 FLOAT(53)
    exp_rate DOUBLE,  -- 使用 DOUBLE 而非 FLOAT(53)
    uplift DOUBLE,  -- 使用 DOUBLE 而非 FLOAT(53)
    uplift_ci_lower DOUBLE,  -- 使用 DOUBLE 而非 FLOAT(53)
    uplift_ci_upper DOUBLE,  -- 使用 DOUBLE 而非 FLOAT(53)
    z_score DOUBLE,  -- 使用 DOUBLE 而非 TEXT
    p_value DOUBLE  -- 使用 DOUBLE 而非 FLOAT(53)
);



SHOW TABLES LIKE 'tb_experiment_vs_control';

