-- Explicit Formula owner LIVE approval v1.
--
-- Additive hardening for the approval table introduced by migration 005.
-- Existing immutable approvals remain readable.  Every new approval must carry
-- the exact formula/runtime schema tuple, the confirmation method, and an
-- audit fingerprint.  Approval is deliberately recorded while delivery is
-- disabled; FORMULA_LIVE_ALERTS_ENABLED remains a separate deployment action.

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS formula_schema_version TEXT;

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS feature_schema_version TEXT;

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS outcome_method_version TEXT;

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS approval_operation_version TEXT;

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS confirmation_method TEXT;

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS approval_request_fingerprint CHAR(64);

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS delivery_environment_enabled BOOLEAN;

CREATE OR REPLACE FUNCTION validate_formula_owner_live_approval()
RETURNS TRIGGER AS $$
DECLARE
    formula_row research_formulas%ROWTYPE;
BEGIN
    IF BTRIM(COALESCE(NEW.formula_schema_version, '')) = ''
       OR BTRIM(COALESCE(NEW.feature_schema_version, '')) = ''
       OR BTRIM(COALESCE(NEW.outcome_method_version, '')) = ''
       OR NEW.approval_operation_version IS DISTINCT FROM
          'formula-owner-live-approval-v1'
       OR NEW.confirmation_method IS NULL
       OR NEW.confirmation_method NOT IN ('EXACT_TYPED', 'ENV_TOKEN')
       OR BTRIM(COALESCE(NEW.approval_request_fingerprint, ''))
          !~ '^[0-9a-f]{64}$'
       OR NEW.delivery_environment_enabled IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION
            'new formula LIVE approvals require v1 owner audit fields and delivery disabled';
    END IF;

    SELECT * INTO formula_row
    FROM research_formulas
    WHERE formula_id=NEW.formula_id
    FOR SHARE;

    IF NOT FOUND
       OR formula_row.active IS DISTINCT FROM TRUE
       OR formula_row.current_stage IS DISTINCT FROM 'SHADOW'
       OR formula_row.formula_version IS DISTINCT FROM NEW.formula_version
       OR formula_row.horizon_minutes IS DISTINCT FROM NEW.horizon_minutes
       OR formula_row.formula_schema_version IS DISTINCT FROM
          NEW.formula_schema_version
       OR formula_row.feature_schema_version IS DISTINCT FROM
          NEW.feature_schema_version
       OR formula_row.outcome_method_version IS DISTINCT FROM
          NEW.outcome_method_version THEN
        RAISE EXCEPTION
            'formula must be an active, schema-identical SHADOW row at approval time';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_formula_owner_live_approval
    ON research_formula_live_approvals;

CREATE TRIGGER trg_validate_formula_owner_live_approval
BEFORE INSERT ON research_formula_live_approvals
FOR EACH ROW EXECUTE FUNCTION validate_formula_owner_live_approval();

CREATE OR REPLACE FUNCTION require_formula_owner_live_approval()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.current_stage='LIVE'
       AND OLD.current_stage IS DISTINCT FROM 'LIVE'
       AND NOT EXISTS (
           SELECT 1
           FROM research_formula_live_approvals approval
           WHERE approval.formula_id=NEW.formula_id
             AND approval.formula_version=NEW.formula_version
             AND approval.formula_schema_version=NEW.formula_schema_version
             AND approval.feature_schema_version=NEW.feature_schema_version
             AND approval.outcome_method_version=NEW.outcome_method_version
             AND approval.approval_operation_version=
                 'formula-owner-live-approval-v1'
             AND approval.delivery_environment_enabled=FALSE
       ) THEN
        RAISE EXCEPTION
            'LIVE transition requires an explicit schema-identical owner approval';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_require_formula_owner_live_approval
    ON research_formulas;

CREATE TRIGGER trg_require_formula_owner_live_approval
BEFORE UPDATE OF current_stage ON research_formulas
FOR EACH ROW EXECUTE FUNCTION require_formula_owner_live_approval();
