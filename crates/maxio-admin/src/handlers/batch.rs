use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use maxio_common::error::MaxioError;

use crate::{
    AdminSys,
    batch::BatchJob,
    handlers::AdminApiError,
    types::BatchJobSubmitRequest,
};

pub async fn submit_batch_job(
    State(admin): State<Arc<AdminSys>>,
    Json(payload): Json<BatchJobSubmitRequest>,
) -> Result<Json<BatchJob>, AdminApiError> {
    let job = admin
        .job_scheduler()
        .submit_job(payload.job_type, payload.expiration)
        .await
        .map_err(AdminApiError::from)?;
    Ok(Json(job))
}

pub async fn list_batch_jobs(
    State(admin): State<Arc<AdminSys>>,
) -> Result<Json<Vec<BatchJob>>, AdminApiError> {
    Ok(Json(admin.job_scheduler().list_jobs().await))
}

pub async fn get_batch_job(
    State(admin): State<Arc<AdminSys>>,
    Path(job_id): Path<String>,
) -> Result<Json<BatchJob>, AdminApiError> {
    admin
        .job_scheduler()
        .get_job(&job_id)
        .await
        .map(Json)
        .ok_or_else(|| {
            AdminApiError(MaxioError::InvalidArgument(format!(
                "batch job not found: {job_id}"
            )))
        })
}

pub async fn cancel_batch_job(
    State(admin): State<Arc<AdminSys>>,
    Path(job_id): Path<String>,
) -> Result<Json<BatchJob>, AdminApiError> {
    let job = admin
        .job_scheduler()
        .cancel_job(&job_id)
        .await
        .map_err(AdminApiError::from)?;
    Ok(Json(job))
}
