use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use axum::response::Response;
use http::{HeaderValue, Request};
use maxio_common::request_id;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct ResponseHeadersLayer;

impl ResponseHeadersLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for ResponseHeadersLayer {
    type Service = ResponseHeadersMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ResponseHeadersMiddleware { inner }
    }
}

#[derive(Clone)]
pub struct ResponseHeadersMiddleware<S> {
    inner: S,
}

impl<S, B> Service<Request<B>> for ResponseHeadersMiddleware<S>
where
    S: Service<Request<B>, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let mut response = inner.call(req).await?;
            let headers = response.headers_mut();

            let request_id = request_id::generate_request_id();
            if let Ok(val) = HeaderValue::from_str(&request_id) {
                headers.insert("x-amz-request-id", val);
            }

            let host_id = request_id::generate_host_id();
            if let Ok(val) = HeaderValue::from_str(&host_id) {
                headers.insert("x-amz-id-2", val);
            }

            headers.insert(
                "server",
                HeaderValue::from_static("MinIO"),
            );

            if !headers.contains_key("accept-ranges") {
                headers.insert("accept-ranges", HeaderValue::from_static("bytes"));
            }

            Ok(response)
        })
    }
}
