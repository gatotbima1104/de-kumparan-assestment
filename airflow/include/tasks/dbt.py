from airflow.providers.standard.operators.bash import BashOperator

from include.constant import DBT_PROJECT_DIR


def _dbt_build(*, task_id: str, target: str, **kwargs):
    """[TASK] Build dbt model"""
    return BashOperator(
        task_id=task_id,
        bash_command=(f"dbt build --select {target} --indirect-selection buildable"),
        cwd=DBT_PROJECT_DIR,
        **kwargs,
    )

def validate_metrics():
    """[TASK] Validate metrics"""
    return BashOperator(
        task_id="validate_semantic_metrics",
        bash_command=(
            "dbt parse --no-partial-parse && mf validate-configs && mf list metrics"
        ),
        cwd=DBT_PROJECT_DIR,
    )

def install_packages():
    """[TASK] Install dbt pakcages"""
    return BashOperator(
        task_id="install_dbt_packages", bash_command="dbt deps", cwd=DBT_PROJECT_DIR
    )
    
def snapshot_authors():
    """[TASK] Track author email history."""
    return BashOperator(
        task_id="snapshot_authors",
        bash_command="dbt snapshot --select snapshot_authors",
        cwd=DBT_PROJECT_DIR,
    )

def build_staging():
    """[TASK] Build staging layer"""
    return _dbt_build(task_id="staging_layer", target="path:models/staging")

def build_intermediate():
    """[TASK] Build intermediate layer"""
    return _dbt_build(task_id="intermediate_layer", target="path:models/intermediate")

def build_marts():
    """[TASK] Build marts layer"""
    return _dbt_build(task_id="marts_layer", target="path:models/marts")
