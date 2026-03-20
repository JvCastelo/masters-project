import json
import logging

from sklearn.metrics import r2_score, root_mean_squared_error

from masters_project.settings import settings

logger = logging.getLogger(__name__)


def evaluate_performance(y_actual, y_predicted, model_name: str):
    """
    Calculates ML metrics (RMSE, R2) and saves them to a JSON report.
    """
    logger.info("Calculating performance metrics...")

    rmse = root_mean_squared_error(y_actual, y_predicted)
    r2 = r2_score(y_actual, y_predicted)

    metrics = {
        "model_type": model_name,
        "station": settings.execution.active_station,
        "start_date": settings.execution.start_date,
        "end_date": settings.execution.end_date,
        "rmse_w_m2": round(rmse, 3),
        "r2_score": round(r2, 3),
    }

    logger.info(
        f"FINAL RESULTS: RMSE = {metrics['rmse_w_m2']} | R2 = {metrics['r2_score']}"
    )

    settings.RESULTS_PATH.mkdir(parents=True, exist_ok=True)

    filename = f"{model_name}_{settings.execution.active_station}_metrics.json"
    report_path = settings.RESULTS_PATH / filename

    with open(report_path, "w") as f:
        json.dump(metrics, f, indent=4)

    logger.info(f"Metrics report saved to {report_path}")

    return
