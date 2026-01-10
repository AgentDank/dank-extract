-- CT Cannabis DuckDB Schema
-- Brings up tables for all CT cannabis datasets

-------------------------------------------------------------------------------
-- Brands (lab-tested cannabis products with cannabinoid/terpene profiles)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ct_brands (
    brand_name TEXT,
    dosage_form TEXT,
    branding_entity TEXT,
    product_image_url TEXT,
    product_image_desc TEXT,
    label_image_url TEXT,
    label_image_desc TEXT,
    lab_analysis_url TEXT,
    lab_analysis_desc TEXT,
    approval_date DATETIME,
    registration_number TEXT NOT NULL,
    tetrahydrocannabinol_thc DOUBLE,
    tetrahydrocannabinol_acid_thca DOUBLE,
    cannabidiols_cbd DOUBLE,
    cannabidiol_acid_cbda DOUBLE,
    a_pinene DOUBLE,
    b_myrcene DOUBLE,
    b_caryophyllene DOUBLE,
    b_pinene DOUBLE,
    limonene DOUBLE,
    ocimene DOUBLE,
    linalool_lin DOUBLE,
    humulene_hum DOUBLE,
    cbg DOUBLE,
    cbg_a DOUBLE,
    cannabavarin_cbdv DOUBLE,
    cannabichromene_cbc DOUBLE,
    cannbinol_cbn DOUBLE,
    tetrahydrocannabivarin_thcv DOUBLE,
    a_bisabolol DOUBLE,
    a_phellandrene DOUBLE,
    a_terpinene DOUBLE,
    b_eudesmol DOUBLE,
    b_terpinene DOUBLE,
    fenchone DOUBLE,
    pulegol DOUBLE,
    borneol DOUBLE,
    isopulegol DOUBLE,
    carene DOUBLE,
    camphene DOUBLE,
    camphor DOUBLE,
    caryophyllene_oxide DOUBLE,
    cedrol DOUBLE,
    eucalyptol DOUBLE,
    geraniol DOUBLE,
    guaiol DOUBLE,
    geranyl_acetate DOUBLE,
    isoborneol DOUBLE,
    menthol DOUBLE,
    l_fenchone DOUBLE,
    nerol DOUBLE,
    sabinene DOUBLE,
    terpineol DOUBLE,
    terpinolene DOUBLE,
    trans_b_farnesene DOUBLE,
    valencene DOUBLE,
    a_cedrene DOUBLE,
    a_farnesene DOUBLE,
    b_farnesene DOUBLE,
    cis_nerolidol DOUBLE,
    fenchol DOUBLE,
    trans_nerolidol DOUBLE,
    market TEXT,
    chemotype TEXT,
    processing_technique TEXT,
    solvents_used TEXT,
    national_drug_code TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ct_brands_reg ON ct_brands (registration_number);
CREATE INDEX IF NOT EXISTS ct_brands_name ON ct_brands (brand_name);
CREATE INDEX IF NOT EXISTS ct_brands_date ON ct_brands (approval_date);

-------------------------------------------------------------------------------
-- Credentials (license credential counts by type and status)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ct_credentials (
    credential_type TEXT NOT NULL,
    status TEXT NOT NULL,
    count INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS ct_credentials_type_status ON ct_credentials (credential_type, status);

-------------------------------------------------------------------------------
-- Applications (cannabis license applications)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ct_applications (
    application_license_number TEXT NOT NULL,
    application_credential_status TEXT,
    status_reason TEXT,
    sec_review_status TEXT,
    initial_application_type TEXT,
    how_selected TEXT,
    name TEXT,
    documents_url TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ct_applications_license ON ct_applications (application_license_number);
CREATE INDEX IF NOT EXISTS ct_applications_status ON ct_applications (application_credential_status);
CREATE INDEX IF NOT EXISTS ct_applications_type ON ct_applications (initial_application_type);

-------------------------------------------------------------------------------
-- Weekly Sales (weekly retail sales data)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ct_weekly_sales (
    week_ending DATETIME NOT NULL,
    adult_use DOUBLE,
    medical DOUBLE,
    total DOUBLE,
    adult_use_products_sold INTEGER,
    medical_products_sold INTEGER,
    total_products_sold INTEGER,
    adult_use_avg_price DOUBLE,
    medical_avg_price DOUBLE
);

CREATE UNIQUE INDEX IF NOT EXISTS ct_weekly_sales_week ON ct_weekly_sales (week_ending);

-------------------------------------------------------------------------------
-- Tax (monthly tax revenue data)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ct_tax (
    period_end_date DATETIME NOT NULL,
    month TEXT,
    year TEXT,
    fiscal_year TEXT,
    plant_material_tax DOUBLE,
    edible_products_tax DOUBLE,
    other_cannabis_tax DOUBLE,
    total_tax DOUBLE
);

CREATE UNIQUE INDEX IF NOT EXISTS ct_tax_period ON ct_tax (period_end_date);
CREATE INDEX IF NOT EXISTS ct_tax_fiscal_year ON ct_tax (fiscal_year);
