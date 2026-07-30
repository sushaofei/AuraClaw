-- 行业均价智能横向比对 DWD 层 MySQL 8.0 DDL
-- 口径：仅保留最终成交价明细，不保存历史采购中间报价过程，不设置 price_event_id。
-- 字符集：utf8mb4；存储引擎：InnoDB。

SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS dwd_pr_price_event_detail_di (
  tenant_id                    VARCHAR(64)    NOT NULL COMMENT '租户ID，所有Agent数据访问的强制隔离键',
  price_line_id                VARCHAR(128)   NOT NULL COMMENT '最终成交价格行稳定ID，不随分区和重跑变化',
  purchase_project_code        VARCHAR(64)    NOT NULL COMMENT '采购项目/订单编码',
  purchase_project_name        VARCHAR(255)   NOT NULL COMMENT '采购项目/订单名称',
  bid_section_id               CHAR(36)       NOT NULL COMMENT '源标段ID',
  bid_section_name             VARCHAR(255)   NULL COMMENT '源标段名称',
  transaction_period           CHAR(7)        NOT NULL COMMENT '成交周期，YYYY-MM',
  transaction_time             DATETIME       NOT NULL COMMENT '最终成交时间；中材测试数据取中标时间',
  org_code                     VARCHAR(64)    NULL COMMENT '采购组织编码',
  org_name                     VARCHAR(255)   NULL COMMENT '采购组织名称',
  region_code                  VARCHAR(64)    NULL COMMENT '采购区域编码',
  category_code                VARCHAR(64)    NULL COMMENT '分析品类代理编码；源表未提供时为空',
  category_name                VARCHAR(255)   NULL COMMENT '一级品类名称',
  subcategory_code             VARCHAR(64)    NULL COMMENT '子品类代理编码；源表未提供时为空',
  subcategory_name             VARCHAR(255)   NULL COMMENT '二级品类名称',
  material_code                VARCHAR(64)    NULL COMMENT '标准物料编码；可由物料明细表materialNo补全',
  material_source_guid         CHAR(36)       NOT NULL COMMENT '物料源GUID',
  material_name                VARCHAR(255)   NOT NULL COMMENT '物料名称',
  spec_model                   VARCHAR(255)   NULL COMMENT '规格型号',
  supplier_code                VARCHAR(64)    NOT NULL COMMENT '源供应商代码',
  supplier_name                VARCHAR(255)   NOT NULL COMMENT '源脱敏供应商名称',
  standard_quantity            DECIMAL(20,6)  NOT NULL COMMENT '最终成交数量',
  standard_uom_code            VARCHAR(32)    NULL COMMENT '标准计量单位；可由物料明细表Meteringunit补全',
  currency_code                CHAR(3)        NOT NULL DEFAULT 'CNY' COMMENT '币种',
  tax_basis_code               VARCHAR(32)    NOT NULL DEFAULT 'UNKNOWN' COMMENT '税价口径',
  current_purchase_unit_price  DECIMAL(20,6)  NOT NULL COMMENT '最终成交单价',
  current_purchase_amount      DECIMAL(20,4)  NOT NULL COMMENT '最终成交金额',
  price_stage_code             VARCHAR(32)    NOT NULL DEFAULT 'FINAL_TRANSACTION' COMMENT '价格阶段',
  current_data_flag            VARCHAR(32)    NOT NULL DEFAULT 'SOURCE_BACKED' COMMENT '最终成交价血缘',
  final_transaction_status     VARCHAR(64)    NOT NULL COMMENT '最终成交确认状态',
  supplier_identity_flag       VARCHAR(64)    NOT NULL COMMENT '供应商身份质量标识',
  data_quality_status          VARCHAR(512)   NOT NULL COMMENT '数据质量状态',
  source_file_name             VARCHAR(255)   NOT NULL COMMENT '来源文件名',
  source_sheet_name            VARCHAR(128)   NOT NULL COMMENT '来源Sheet',
  source_excel_row             INT UNSIGNED   NOT NULL COMMENT '来源Excel行号',
  etl_batch_no                 VARCHAR(64)    NOT NULL COMMENT 'ETL批次号',
  etl_load_time                DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL装载时间',
  dt                           DATE           NOT NULL COMMENT '数据分区日期',
  PRIMARY KEY (
    tenant_id,
    price_line_id,
    dt
  ),
  KEY idx_dwd_price_project (tenant_id, purchase_project_code, transaction_period),
  KEY idx_dwd_price_material (tenant_id, material_source_guid, supplier_code),
  KEY idx_dwd_price_batch (etl_batch_no),
  CONSTRAINT chk_dwd_price_quantity CHECK (standard_quantity > 0),
  CONSTRAINT chk_dwd_price_unit_price CHECK (current_purchase_unit_price > 0),
  CONSTRAINT chk_dwd_price_amount CHECK (current_purchase_amount >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='最终成交价DWD明细；表名暂沿用现有模型命名';

CREATE TABLE IF NOT EXISTS dwd_pr_industry_price_benchmark_di (
  tenant_id                    VARCHAR(64)    NOT NULL COMMENT '租户ID；共享行业源也须投影为租户可见版本',
  benchmark_id                 VARCHAR(64)    NOT NULL COMMENT '行业基准编号',
  benchmark_version            VARCHAR(64)    NOT NULL COMMENT '行业基准版本',
  benchmark_period             CHAR(7)        NOT NULL COMMENT '基准周期，YYYY-MM',
  industry_source_code         VARCHAR(128)   NOT NULL COMMENT '行业数据源编码',
  source_price_record_id       VARCHAR(128)   NULL COMMENT '外部来源价格记录ID',
  category_code                VARCHAR(64)    NULL COMMENT '分析品类代理编码',
  category_name                VARCHAR(255)   NULL COMMENT '一级品类名称',
  subcategory_code             VARCHAR(64)    NULL COMMENT '子品类代理编码',
  subcategory_name             VARCHAR(255)   NULL COMMENT '二级品类名称',
  material_code                VARCHAR(64)    NULL COMMENT '标准物料编码',
  material_name                VARCHAR(255)   NOT NULL COMMENT '标准物料名称',
  spec_model                   VARCHAR(255)   NULL COMMENT '规格型号',
  region_code                  VARCHAR(64)    NULL COMMENT '适用区域编码',
  standard_uom_code            VARCHAR(32)    NOT NULL COMMENT '标准计量单位',
  currency_code                CHAR(3)        NOT NULL COMMENT '币种',
  tax_basis_code               VARCHAR(32)    NOT NULL COMMENT '税价口径',
  industry_min_unit_price      DECIMAL(20,6)  NULL COMMENT '行业最低价',
  industry_avg_unit_price      DECIMAL(20,6)  NOT NULL COMMENT '行业基准价；正式数据为行业均价，演示数据可按版本声明为中位价',
  benchmark_statistic_type     VARCHAR(32)    NOT NULL COMMENT '基准统计口径：MEAN/P50等，禁止名称与算法混用',
  industry_max_unit_price      DECIMAL(20,6)  NULL COMMENT '行业最高价',
  industry_sample_count        INT UNSIGNED   NULL COMMENT '行业有效样本数',
  confidence_level             VARCHAR(32)    NOT NULL COMMENT '基准可信等级',
  effective_start_date         DATE           NOT NULL COMMENT '有效开始日期',
  effective_end_date           DATE           NULL COMMENT '有效结束日期',
  industry_data_flag           VARCHAR(32)    NOT NULL COMMENT '行业价血缘标识',
  data_quality_status          VARCHAR(512)   NOT NULL COMMENT '数据质量状态',
  etl_batch_no                 VARCHAR(64)    NOT NULL COMMENT 'ETL批次号',
  etl_load_time                DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL装载时间',
  dt                           DATE           NOT NULL COMMENT '数据分区日期',
  PRIMARY KEY (tenant_id, benchmark_id),
  KEY idx_dwd_benchmark_match (
    tenant_id,
    benchmark_period,
    material_code,
    region_code,
    standard_uom_code,
    currency_code,
    tax_basis_code
  ),
  KEY idx_dwd_benchmark_batch (tenant_id, etl_batch_no),
  CONSTRAINT chk_dwd_benchmark_avg CHECK (industry_avg_unit_price > 0),
  CONSTRAINT chk_dwd_benchmark_range CHECK (
    (industry_min_unit_price IS NULL OR industry_min_unit_price <= industry_avg_unit_price)
    AND (industry_max_unit_price IS NULL OR industry_avg_unit_price <= industry_max_unit_price)
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='外部行业价格基准DWD明细';

CREATE TABLE IF NOT EXISTS dwd_pr_price_compare_pair_di (
  tenant_id                    VARCHAR(64)    NOT NULL COMMENT '租户ID，所有Agent数据访问的强制隔离键',
  compare_pair_id              VARCHAR(128)   NOT NULL COMMENT '成交价与行业基准匹配证据稳定ID',
  price_line_id                VARCHAR(128)   NOT NULL COMMENT '关联最终成交价格行稳定ID',
  purchase_project_code        VARCHAR(64)    NOT NULL COMMENT '采购项目/订单编码',
  bid_section_id               CHAR(36)       NOT NULL COMMENT '源标段ID',
  transaction_period           CHAR(7)        NOT NULL COMMENT '成交周期，YYYY-MM',
  org_code                     VARCHAR(64)    NULL COMMENT '采购组织编码',
  region_code                  VARCHAR(64)    NULL COMMENT '采购区域编码',
  category_code                VARCHAR(64)    NULL COMMENT '分析品类代理编码',
  category_name                VARCHAR(255)   NULL COMMENT '一级品类名称',
  subcategory_code             VARCHAR(64)    NULL COMMENT '子品类代理编码',
  subcategory_name             VARCHAR(255)   NULL COMMENT '二级品类名称',
  material_code                VARCHAR(64)    NULL COMMENT '标准物料编码',
  material_source_guid         CHAR(36)       NOT NULL COMMENT '物料源GUID',
  material_name                VARCHAR(255)   NOT NULL COMMENT '物料名称',
  spec_model                   VARCHAR(255)   NULL COMMENT '规格型号',
  supplier_code                VARCHAR(64)    NOT NULL COMMENT '源供应商代码',
  supplier_name                VARCHAR(255)   NOT NULL COMMENT '源脱敏供应商名称',
  standard_quantity            DECIMAL(20,6)  NOT NULL COMMENT '最终成交数量',
  standard_uom_code            VARCHAR(32)    NULL COMMENT '标准计量单位',
  currency_code                CHAR(3)        NOT NULL COMMENT '币种',
  tax_basis_code               VARCHAR(32)    NOT NULL COMMENT '税价口径',
  current_purchase_unit_price  DECIMAL(20,6)  NOT NULL COMMENT '最终成交价',
  current_purchase_amount      DECIMAL(20,4)  NOT NULL COMMENT '最终成交金额',
  current_data_flag            VARCHAR(32)    NOT NULL COMMENT '最终成交价血缘',
  benchmark_id                 VARCHAR(64)    NULL COMMENT '选中的行业基准编号',
  benchmark_version            VARCHAR(64)    NULL COMMENT '行业基准版本',
  benchmark_period             CHAR(7)        NULL COMMENT '行业基准周期',
  industry_source_code         VARCHAR(128)   NULL COMMENT '行业数据源编码',
  industry_min_unit_price      DECIMAL(20,6)  NULL COMMENT '行业最低价',
  industry_avg_unit_price      DECIMAL(20,6)  NULL COMMENT '行业基准价；正式数据为行业均价，演示数据可按版本声明为中位价',
  benchmark_statistic_type     VARCHAR(32)    NULL COMMENT '所选基准统计口径：MEAN/P50等',
  industry_max_unit_price      DECIMAL(20,6)  NULL COMMENT '行业最高价',
  industry_sample_count        INT UNSIGNED   NULL COMMENT '行业样本数',
  confidence_level             VARCHAR(32)    NULL COMMENT '基准可信等级',
  industry_data_flag           VARCHAR(32)    NOT NULL COMMENT '行业价血缘标识',
  benchmark_match_rule_code    VARCHAR(128)   NOT NULL COMMENT '行业基准匹配规则编码',
  benchmark_match_status       VARCHAR(64)    NOT NULL COMMENT '行业基准匹配状态',
  is_selected                  TINYINT(1)     NOT NULL DEFAULT 0 COMMENT '是否选中基准',
  uncomparable_reason_code     VARCHAR(128)   NULL COMMENT '不可比原因编码',
  material_match_score         DECIMAL(8,6)   NULL COMMENT '标准物料匹配置信分，0到1',
  material_mapping_version     VARCHAR(64)    NULL COMMENT '物料映射规则/模型版本',
  rule_version                 VARCHAR(64)    NOT NULL COMMENT '比对规则版本',
  data_quality_status          VARCHAR(512)   NOT NULL COMMENT '综合数据质量状态',
  etl_batch_no                 VARCHAR(64)    NOT NULL COMMENT 'ETL批次号',
  etl_load_time                DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL装载时间',
  dt                           DATE           NOT NULL COMMENT '数据分区日期',
  PRIMARY KEY (
    tenant_id,
    compare_pair_id,
    dt
  ),
  KEY idx_dwd_pair_project (tenant_id, purchase_project_code, transaction_period),
  KEY idx_dwd_pair_price_line (tenant_id, price_line_id),
  KEY idx_dwd_pair_benchmark (tenant_id, benchmark_id),
  KEY idx_dwd_pair_status (benchmark_match_status, uncomparable_reason_code),
  KEY idx_dwd_pair_batch (etl_batch_no),
  CONSTRAINT chk_dwd_pair_selected CHECK (is_selected IN (0, 1)),
  CONSTRAINT chk_dwd_pair_match_score CHECK (
    material_match_score IS NULL
    OR (material_match_score >= 0 AND material_match_score <= 1)
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='最终成交价与行业基准匹配证据DWD明细';

CREATE TABLE IF NOT EXISTS dwd_pr_price_insight_rule_di (
  tenant_id                    VARCHAR(64)    NOT NULL COMMENT '租户ID',
  rule_version                 VARCHAR(64)    NOT NULL COMMENT '规则版本',
  rule_code                    VARCHAR(128)   NOT NULL COMMENT '规则编码',
  anchor_type                  VARCHAR(32)    NOT NULL COMMENT '锚点：HISTORY/REGION/MARKET',
  deviation_threshold_pct      DECIMAL(10,4)  NOT NULL COMMENT '偏离判定阈值百分比',
  min_benchmark_sample_count   INT UNSIGNED   NULL COMMENT '行业基准最小样本数',
  min_material_match_score     DECIMAL(8,6)   NULL COMMENT '物料匹配最低置信分',
  effective_start_time         DATETIME       NOT NULL COMMENT '生效时间',
  effective_end_time           DATETIME       NULL COMMENT '失效时间',
  enabled                      TINYINT(1)      NOT NULL DEFAULT 1 COMMENT '是否启用',
  etl_batch_no                 VARCHAR(64)    NOT NULL COMMENT 'ETL批次号',
  etl_load_time                DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL装载时间',
  dt                           DATE           NOT NULL COMMENT '数据分区日期',
  PRIMARY KEY (tenant_id, rule_version, rule_code, dt),
  CONSTRAINT chk_dwd_rule_threshold CHECK (deviation_threshold_pct >= 0),
  CONSTRAINT chk_dwd_rule_match_score CHECK (
    min_material_match_score IS NULL
    OR (min_material_match_score >= 0 AND min_material_match_score <= 1)
  ),
  CONSTRAINT chk_dwd_rule_enabled CHECK (enabled IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='价格洞察版本化阈值与可比规则DWD明细';
