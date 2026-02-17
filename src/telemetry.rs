use opentelemetry::{global, KeyValue};
use opentelemetry::sdk::{propagation::TraceContextPropagator, Resource, trace as sdktrace};
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::{prelude::*, Registry};
use tracing_subscriber::fmt::format::FmtSpan;
use rdkafka::message::Headers;

pub fn init_telemetry(service_name: &str, endpoint: &str) -> anyhow::Result<()> {
    // 1. Setup Propagator
    global::set_text_map_propagator(TraceContextPropagator::new());

    // 2. Setup OTLP Exporter (HTTP)
    let exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_endpoint(endpoint);

    // 3. Setup Tracer Provider & Tracer
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            sdktrace::config()
                .with_resource(Resource::new(vec![KeyValue::new("service.name", service_name.to_string())])),
        )
        .install_batch(opentelemetry::runtime::Tokio)?;

    // 4. Wrap with tracing-opentelemetry layer
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // 5. Setup Subscriber
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .with_target(false);
    
    let env_filter = tracing_subscriber::EnvFilter::from_default_env();

    Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    Ok(())
}

pub fn shutdown_telemetry() {
    global::shutdown_tracer_provider();
}

/// Helper to extract context from Kafka headers
pub struct KafkaHeaderExtractor<'a>(pub &'a rdkafka::message::BorrowedHeaders);

impl<'a> opentelemetry::propagation::Extractor for KafkaHeaderExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        for i in 0..self.0.count() {
            let header = self.0.get(i);
            if header.key == key {
                return header.value.and_then(|v| std::str::from_utf8(v).ok());
            }
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(self.0.count());
        for i in 0..self.0.count() {
            let header = self.0.get(i);
            keys.push(header.key);
        }
        keys
    }
}
