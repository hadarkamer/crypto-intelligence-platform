-- Formula owner LIVE approval v2: bind the exact engine/runtime contract.
--
-- Approval remains append-only and delivery remains a separate deployment
-- action.  A protected SHADOW/APPROVED/LIVE/RETIRED formula definition cannot
-- be rewritten underneath its prospective evidence or approval record.

ALTER TABLE research_formula_live_approvals
    ADD COLUMN IF NOT EXISTS engine_version TEXT;

ALTER TABLE research_formula_live_approvals
    DROP CONSTRAINT IF EXISTS
    research_formula_live_approvals_formula_id_formula_version_key;

CREATE UNIQUE INDEX IF NOT EXISTS
    idx_formula_live_approvals_exact_runtime
ON research_formula_live_approvals (
    formula_id, formula_version, formula_schema_version, engine_version,
    feature_schema_version, outcome_method_version
)
WHERE engine_version IS NOT NULL;

CREATE OR REPLACE FUNCTION validate_formula_owner_live_approval()
RETURNS TRIGGER AS $$
DECLARE
    formula_row research_formulas%ROWTYPE;
BEGIN
    IF BTRIM(COALESCE(NEW.formula_schema_version, '')) = ''
       OR BTRIM(COALESCE(NEW.engine_version, '')) = ''
       OR BTRIM(COALESCE(NEW.feature_schema_version, '')) = ''
       OR BTRIM(COALESCE(NEW.outcome_method_version, '')) = ''
       OR NEW.approval_operation_version IS DISTINCT FROM
          'formula-owner-live-approval-v2-engine-bound'
       OR NEW.confirmation_method IS NULL
       OR NEW.confirmation_method NOT IN ('EXACT_TYPED', 'ENV_TOKEN')
       OR BTRIM(COALESCE(NEW.approval_request_fingerprint, ''))
          !~ '^[0-9a-f]{64}$'
       OR NEW.delivery_environment_enabled IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION
            'new formula LIVE approvals require v2 exact runtime fields and delivery disabled';
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
       OR formula_row.engine_version IS DISTINCT FROM NEW.engine_version
       OR formula_row.feature_schema_version IS DISTINCT FROM
          NEW.feature_schema_version
       OR formula_row.outcome_method_version IS DISTINCT FROM
          NEW.outcome_method_version THEN
        RAISE EXCEPTION
            'formula must be an active, exact-runtime SHADOW row at approval time';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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
             AND approval.engine_version=NEW.engine_version
             AND approval.feature_schema_version=NEW.feature_schema_version
             AND approval.outcome_method_version=NEW.outcome_method_version
             AND approval.approval_operation_version=
                 'formula-owner-live-approval-v2-engine-bound'
             AND approval.delivery_environment_enabled=FALSE
       ) THEN
        RAISE EXCEPTION
            'LIVE transition requires an explicit exact-runtime owner approval';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION prevent_protected_formula_contract_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF (
           NEW.current_stage IN ('SHADOW', 'APPROVED', 'LIVE')
           AND NEW.active IS DISTINCT FROM TRUE
       ) OR (
           NEW.current_stage='RETIRED'
           AND NEW.active IS DISTINCT FROM FALSE
       ) THEN
        RAISE EXCEPTION
            'protected formula active state is inconsistent with lifecycle stage';
    END IF;

    IF (
           OLD.current_stage='SHADOW'
           AND NEW.current_stage NOT IN ('SHADOW', 'LIVE', 'RETIRED')
       ) OR (
           OLD.current_stage='APPROVED'
           AND NEW.current_stage NOT IN ('APPROVED', 'LIVE', 'RETIRED')
       ) OR (
           OLD.current_stage='LIVE'
           AND NEW.current_stage NOT IN ('LIVE', 'RETIRED')
       ) OR (
           OLD.current_stage='RETIRED'
           AND NEW.current_stage<>'RETIRED'
       ) THEN
        RAISE EXCEPTION
            'protected formula stage cannot be downgraded or reactivated';
    END IF;

    IF OLD.current_stage IN ('SHADOW', 'APPROVED', 'LIVE', 'RETIRED')
       AND (
           NEW.formula_key IS DISTINCT FROM OLD.formula_key
           OR NEW.formula_version IS DISTINCT FROM OLD.formula_version
           OR NEW.formula_schema_version IS DISTINCT FROM
              OLD.formula_schema_version
           OR NEW.engine_version IS DISTINCT FROM OLD.engine_version
           OR NEW.feature_schema_version IS DISTINCT FROM
              OLD.feature_schema_version
           OR NEW.outcome_method_version IS DISTINCT FROM
              OLD.outcome_method_version
           OR NEW.direction IS DISTINCT FROM OLD.direction
           OR NEW.horizon_minutes IS DISTINCT FROM OLD.horizon_minutes
           OR NEW.conditions IS DISTINCT FROM OLD.conditions
           OR NEW.condition_count IS DISTINCT FROM OLD.condition_count
           OR NEW.formula_text IS DISTINCT FROM OLD.formula_text
       ) THEN
        RAISE EXCEPTION
            'protected formula runtime contract is immutable';
    END IF;

    IF OLD.current_stage IN ('APPROVED', 'LIVE', 'RETIRED')
       AND (
           NEW.live_alert_approved IS DISTINCT FROM OLD.live_alert_approved
           OR NEW.live_alert_approved_at_utc IS DISTINCT FROM
              OLD.live_alert_approved_at_utc
           OR NEW.live_alert_approved_by IS DISTINCT FROM
              OLD.live_alert_approved_by
           OR NEW.live_alert_policy_version IS DISTINCT FROM
              OLD.live_alert_policy_version
           OR NEW.shadow_validation_metrics IS DISTINCT FROM
              OLD.shadow_validation_metrics
       ) THEN
        RAISE EXCEPTION
            'protected formula approval evidence is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_protected_formula_contract_mutation
    ON research_formulas;

CREATE TRIGGER trg_prevent_protected_formula_contract_mutation
BEFORE UPDATE ON research_formulas
FOR EACH ROW EXECUTE FUNCTION prevent_protected_formula_contract_mutation();

COMMENT ON COLUMN research_formula_live_approvals.engine_version IS
    'Exact Formula engine version reviewed by the owner; v2 approvals require it.';
